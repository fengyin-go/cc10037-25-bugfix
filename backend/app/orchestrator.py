"""ETL编排中心 — 跨模块业务编排层。

将各引擎组合成完整业务链路：
- 管线创建 → DAG拓扑验证 → 节点登记 → 质量规则绑定 → 调度激活
- Schema变更 → 兼容性评估 → 影响管线遍历 → 配置迁移 → 回滚方案
- 水位延迟 → 回刷补偿 → 分区拆分 → 资源分配 → 进度追踪
- 质量校验 → 异常捕获 → 告警匹配 → 阻断/通过决策
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.engine.alert_matcher import AlertLevel, AlertMatcher, AlertResult, AlertRule
from app.engine.backfill_plan import BackfillEngine, BackfillPlan
from app.engine.dag_topology import DagNode, DagTopology, TopologyResult
from app.engine.lineage_graph import (
    ImpactAnalysis, LineageEdge, LineageGraphEngine, LineageNode,
)
from app.engine.resource_load_balancer import (
    AllocationResult, ResourceLoadBalancer, ResourcePool, TaskResource,
)
from app.engine.schema_evolution import (
    Compatibility, SchemaChange, SchemaColumn, SchemaDefinition,
    SchemaEvolutionEngine,
)
from app.engine.validation_executor import (
    CheckLevel, CheckType, CollectionStatus, PartitionSample, QualityRule,
    ValidationEngine, ValidationResult,
)
from app.engine.watermark_progress import (
    WatermarkAdvanceResult, WatermarkEngine, WatermarkRecord, WatermarkStatus,
)


@dataclass
class PipelineCreateContext:
    """创建ETL管线的完整上下文。"""
    pipeline_id: str
    pipeline_name: str
    source_table: str
    target_table: str
    stages: list[str]
    nodes: list[DagNode] = field(default_factory=list)
    quality_rules: list[QualityRule] = field(default_factory=list)
    topology_result: TopologyResult | None = None


@dataclass
class QualityGateResponse:
    """质量门禁结果。"""
    pipeline_id: str
    overall_pass: bool
    decision: str                     # RELEASE / BLOCKED / PENDING_COLLECTION
    failed_ratio: float
    results: list[dict[str, Any]]
    triggered_alerts: list[AlertResult]
    samples_collected: bool = True
    genuine_empty: bool = False
    coverage: dict[str, Any] | None = None
    partition_results: list[dict[str, Any]] = field(default_factory=list)
    pending_collection: list[dict[str, Any]] = field(default_factory=list)
    collection_failures: list[dict[str, Any]] = field(default_factory=list)
    total_sample_rows: int = 0
    total_bad_rows: int = 0


@dataclass
class BackfillOrchestrateResult:
    """回刷编排结果。"""
    pipeline_id: str
    plan: BackfillPlan
    allocation: AllocationResult | None = None
    quality_gate: QualityGateResponse | None = None


@dataclass
class SchemaChangeOrchestrateResult:
    """Schema变更编排结果。"""
    change: SchemaChange
    affected_pipelines: list[str]
    migration_recommendation: str
    impact_summary: dict[str, Any]


class ETLOrchestrator:
    """ETL编排总控制器。

    组合所有引擎，提供完整业务链路操作入口。
    """

    def __init__(self) -> None:
        self.dag = DagTopology()
        self.validation = ValidationEngine()
        self.watermark = WatermarkEngine()
        self.backfill = BackfillEngine()
        self.lineage = LineageGraphEngine()
        self.balancer = ResourceLoadBalancer()
        self.schema_evolution = SchemaEvolutionEngine()
        self.alert = AlertMatcher()

        # 注册默认资源池
        self.balancer.register_pool(ResourcePool(
            pool_id="pool_prod", pool_name="生产资源池",
            total_cores=128, total_memory_gb=512, max_concurrency=50,
        ))
        self.balancer.register_pool(ResourcePool(
            pool_id="pool_test", pool_name="测试资源池",
            total_cores=32, total_memory_gb=128, max_concurrency=20,
        ))
        self.balancer.register_pool(ResourcePool(
            pool_id="pool_adhoc", pool_name="临时计算池",
            total_cores=16, total_memory_gb=64, max_concurrency=10,
        ))

    # ---- 管线编排 ----

    def create_pipeline(
        self, ctx: PipelineCreateContext
    ) -> dict[str, Any]:
        """创建ETL管线：建节点 → DAG验证 → 质量规则绑定。"""
        for node in ctx.nodes:
            self.dag.add_node(node)

        # 按阶段连接节点（每个阶段之间全连接）
        stage_nodes: dict[str, list[str]] = {}
        for node in ctx.nodes:
            stage = node.meta.get("stage", "")
            stage_nodes.setdefault(stage, []).append(node.node_id)

        stage_order = sorted(stage_nodes.keys())
        for i in range(len(stage_order) - 1):
            for from_id in stage_nodes[stage_order[i]]:
                for to_id in stage_nodes[stage_order[i + 1]]:
                    self.dag.add_edge(from_id, to_id)

        # 拓扑验证
        topology = self.dag.solve()
        ctx.topology_result = topology

        return {
            "pipeline_id": ctx.pipeline_id,
            "pipeline_name": ctx.pipeline_name,
            "source_table": ctx.source_table,
            "target_table": ctx.target_table,
            "node_count": len(ctx.nodes),
            "stage_count": len(stage_order),
            "has_cycle": topology.has_cycle,
            "topology_order": topology.order,
            "critical_path": topology.critical_path,
            "max_depth": max(topology.node_depth.values()) if topology.node_depth else 0,
            "created_at": datetime.now().isoformat(),
        }

    def create_pipeline_with_full_orchestration(
        self,
        pipeline_id: str,
        pipeline_name: str,
        source_table: str,
        target_table: str,
        stages: list[dict[str, Any]],
        quality_rules: list[QualityRule] | None = None,
    ) -> dict[str, Any]:
        """完整管线编排：DAG + 质量 + 调度 + 血缘。"""
        # 建节点
        nodes: list[DagNode] = []
        for stage in stages:
            for node_def in stage.get("nodes", []):
                nodes.append(DagNode(
                    node_id=node_def["id"],
                    name=node_def["name"],
                    weight=node_def.get("weight", 1.0),
                    meta={"stage": stage["name"], **node_def.get("meta", {})},
                ))

        ctx = PipelineCreateContext(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            source_table=source_table,
            target_table=target_table,
            stages=[s["name"] for s in stages],
            nodes=nodes,
            quality_rules=quality_rules or [],
        )
        result = self.create_pipeline(ctx)

        # 登记血缘
        self.lineage.add_node(LineageNode(source_table, source_table, node_type="source"))
        self.lineage.add_node(LineageNode(target_table, target_table, node_type="target"))
        self.lineage.add_edge(LineageEdge(
            edge_id=f"edge_{source_table}_to_{target_table}",
            source=source_table,
            target=target_table,
            transfer_fields=[n.name for n in nodes if n.meta.get("stage") == stages[0]["name"]],
            transform_type="etl_pipeline",
        ))

        result["lineage_edge"] = f"edge_{source_table}_to_{target_table}"
        result["quality_rules_count"] = len(ctx.quality_rules)

        return result

    # ---- 质量门禁 ----

    @staticmethod
    def _detect_missing_credentials(sample_data: list[dict[str, Any]]) -> bool:
        """识别入口收到的是否为缺失采集凭据的空样本。

        没有任何样本行（空列表）视为采集凭据缺失/未采集：
        真实零行会通过分区接口以 COLLECTED + 空 rows 显式表达，
        不会走这个裸入口。
        """
        return len(sample_data) == 0

    def run_quality_gate(
        self,
        pipeline_id: str,
        rules: list[QualityRule],
        sample_data: list[dict[str, Any]],
    ) -> QualityGateResponse:
        """执行管线质量门禁。

        裸入口收到空样本时按「采集凭据缺失」处理：没有有效质检结论，
        门禁不通过（PENDING_COLLECTION），校验详情不补造样本行。
        """
        samples_collected = not self._detect_missing_credentials(sample_data)
        result = self.validation.run_pipeline_quality(
            rules, sample_data, pipeline_id,
            samples_collected=samples_collected,
        )

        alerts: list[AlertResult] = []
        if not samples_collected:
            # 采集待恢复记录：与坏行告警分开，不伪造 failed_ratio
            coverage_rule = AlertRule(
                rule_id="qalert_collection_coverage",
                rule_name="采集覆盖缺失告警",
                monitor_target=pipeline_id,
                condition="coverage_pct < 1",
                level=AlertLevel.CRITICAL,
            )
            alerts = self.alert.match_batch(
                [coverage_rule],
                {"coverage_pct": 0.0, "failed_ratio": 0.0},
            )
        else:
            # 告警评估使用逐规则的实际覆盖与坏行统计，而不是被零行稀释的汇总值
            alert_rules = [
                AlertRule(
                    rule_id=f"qalert_{r.rule_id}",
                    rule_name=f"质量检查:{r.rule_name}",
                    monitor_target=pipeline_id,
                    condition=f"failed_ratio > {r.threshold}",
                    level=AlertLevel.WARN if r.level == CheckLevel.WARN else AlertLevel.CRITICAL,
                )
                for r in rules
            ]
            for rule, summary in zip(rules, result["rule_results"]):
                metrics = {
                    "failed_ratio": summary["ratio"],
                    "failed_rows": summary["failed"],
                    "total_rows": summary["total"],
                }
                alerts.extend(self.alert.match_batch(
                    [next(ar for ar in alert_rules if ar.rule_id == f"qalert_{rule.rule_id}")],
                    metrics,
                ))

        return QualityGateResponse(
            pipeline_id=pipeline_id,
            overall_pass=result["overall_pass"],
            decision=result["decision"],
            failed_ratio=result["overall_failed_ratio"],
            results=result["rule_results"],
            triggered_alerts=alerts,
            samples_collected=result["samples_collected"],
            genuine_empty=result["genuine_empty"],
            total_sample_rows=sum(r["total"] for r in result["rule_results"]),
            total_bad_rows=sum(r["failed"] for r in result["rule_results"]),
        )

    def run_partition_quality_gate(
        self,
        pipeline_id: str,
        rules: list[QualityRule],
        partitions: list[PartitionSample],
    ) -> QualityGateResponse:
        """分区质检门禁：先校验采集覆盖，再评估逐规则坏行与告警。

        告警分两类，值班人员可据此区分「可立即处理的坏行」与「等待采集
        恢复的分区」：

        - 采集覆盖告警：coverage_pct < 1，携带未回报/失败分区清单；
        - 坏行告警：按每条规则在实际到齐样本上的 failed_ratio 与原阈值评估，
          只统计已确认为坏行的订单记录，真实零行不进入分母。
        """
        result = self.validation.run_partition_gate(rules, partitions, pipeline_id)
        coverage = result["coverage"]
        alerts: list[AlertResult] = []

        # 1) 采集覆盖告警（待恢复记录，不是坏行）
        if not coverage["coverage_complete"]:
            coverage_rule = AlertRule(
                rule_id="qalert_collection_coverage",
                rule_name="采集覆盖缺失告警",
                monitor_target=pipeline_id,
                condition="coverage_pct < 1",
                level=AlertLevel.CRITICAL,
            )
            coverage_alerts = self.alert.match_batch(
                [coverage_rule],
                {
                    "coverage_pct": coverage["coverage_pct"],
                    "failed_ratio": result["overall_failed_ratio"],
                },
            )
            for alert in coverage_alerts:
                if alert.matched:
                    pending = (
                        coverage["not_collected_partitions"]
                        + coverage["collection_failed_partitions"]
                    )
                    alert.message = (
                        f"{alert.message}\n待恢复分区: {', '.join(pending) or '无'}"
                    )
            alerts.extend(coverage_alerts)

        # 2) 逐规则坏行告警：只用实际到齐样本的统计与规则原阈值
        for summary in result["rule_results"]:
            alert_rule = AlertRule(
                rule_id=f"qalert_{summary['rule_id']}",
                rule_name=f"质量检查:{summary['rule_name']}",
                monitor_target=pipeline_id,
                condition=f"failed_ratio > {summary['threshold']}",
                level=AlertLevel.WARN if summary["level"] == "WARN" else AlertLevel.CRITICAL,
            )
            rule_alerts = self.alert.match_batch(
                [alert_rule],
                {
                    "failed_ratio": summary["failed_ratio"],
                    "failed_rows": summary["failed_rows"],
                    "total_rows": summary["total_rows"],
                },
            )
            for alert in rule_alerts:
                if alert.matched and summary["bad_partitions"]:
                    alert.message = (
                        f"{alert.message}\n坏行分区: "
                        f"{', '.join(summary['bad_partitions'])}"
                    )
            alerts.extend(rule_alerts)

        return QualityGateResponse(
            pipeline_id=pipeline_id,
            overall_pass=result["overall_pass"],
            decision=result["decision"],
            failed_ratio=result["overall_failed_ratio"],
            results=result["rule_results"],
            triggered_alerts=alerts,
            samples_collected=coverage["coverage_complete"],
            genuine_empty=(
                coverage["coverage_complete"] and result["total_sample_rows"] == 0
            ),
            coverage=coverage,
            partition_results=result["partition_results"],
            pending_collection=result["pending_collection"],
            collection_failures=result["collection_failures"],
            total_sample_rows=result["total_sample_rows"],
            total_bad_rows=result["total_bad_rows"],
        )

    # ---- 水位与回刷 ----

    def check_watermark_and_backfill(
        self,
        record: WatermarkRecord,
        new_watermark: str,
        backfill_threshold: int = 3,
    ) -> dict[str, Any]:
        """检查水位并决定是否触发回刷。"""
        advance = self.watermark.advance(record, new_watermark)

        need_backfill, backfill_reason = self.watermark.should_trigger_backfill(
            record, backfill_threshold,
        )

        result: dict[str, Any] = {
            "pipeline_id": record.pipeline_id,
            "old_watermark": advance.old_watermark,
            "new_watermark": advance.new_watermark,
            "delay_minutes": advance.delay_minutes,
            "new_status": advance.new_status.value,
            "need_backfill": need_backfill,
            "backfill_reason": backfill_reason or "",
        }

        if need_backfill:
            try:
                from datetime import datetime, timedelta
                wm_dt = datetime.strptime(record.current_watermark, record.partition_format)
                expected = datetime.now() - timedelta(days=1)
                plan = self.backfill.plan(
                    pipeline_id=record.pipeline_id,
                    start_partition=record.current_watermark,
                    end_partition=expected.strftime(record.partition_format),
                    parallelism=4,
                    reason=backfill_reason or "水位延迟自动触发",
                )
                result["backfill_plan"] = {
                    "total_partitions": plan.total_partitions,
                    "batch_count": len(plan.batches),
                    "parallelism": plan.parallelism,
                    "estimated_completion": plan.estimated_completion,
                }
            except ValueError:
                result["backfill_plan"] = {"error": "无法生成回刷方案"}

        return result

    # ---- Schema变更 ----

    def evaluate_schema_change(
        self,
        change_id: str,
        before: SchemaDefinition,
        after: SchemaDefinition,
        pipeline_deps: dict[str, list[str]],
    ) -> SchemaChangeOrchestrateResult:
        """评估Schema变更并生成完整影响报告。"""
        change = self.schema_evolution.evaluate_change(
            change_id, before, after, pipeline_deps,
        )

        # 遍历受影响的管线
        affected = self.schema_evolution.walk_affected_pipelines(
            before.table_name, pipeline_deps,
        )

        migration_rec = ""
        if change.compatibility == Compatibility.INCOMPATIBLE:
            migration_rec = (
                f"不兼容变更，请联系 {len(change.affected_pipelines)} 条受影响管线的负责人，"
                f"先调整下游ETL逻辑再执行Schema变更。回滚方案：{change.rollback_plan[:200]}..."
            )
        elif change.compatibility == Compatibility.COMPATIBLE:
            migration_rec = "兼容变更，可直接部署，无需修改下游管线。"
        else:
            migration_rec = "待评估，请确认变更详情后再决定。"

        return SchemaChangeOrchestrateResult(
            change=change,
            affected_pipelines=[item["table"] for item in affected],
            migration_recommendation=migration_rec,
            impact_summary={
                "change_id": change_id,
                "table": before.table_name,
                "compatibility": change.compatibility.value,
                "total_changes": len(change.detail.get("changes", [])),
                "affected_pipeline_count": len(change.affected_pipelines),
                "rollback_available": bool(change.rollback_plan),
            },
        )

    # ---- 资源分配 ----

    def allocate_for_pipeline(
        self, pipeline_id: str, node_count: int, estimated_memory_gb: float
    ) -> dict[str, Any]:
        """为管线分配计算资源。"""
        task = TaskResource(
            task_id=f"task_{pipeline_id}",
            required_cores=min(node_count * 2, 32),
            required_memory_gb=estimated_memory_gb,
            priority=1,
            labels={"pipeline": pipeline_id},
        )
        result = self.balancer.allocate(task)

        health = self.balancer.cluster_health()
        return {
            "pipeline_id": pipeline_id,
            "allocation": {
                "allocated": result.allocated,
                "pool_id": result.pool_id,
                "pool_name": result.pool_name,
                "reason": result.reason,
            },
            "cluster_health": health,
            "required_cores": task.required_cores,
            "required_memory_gb": task.required_memory_gb,
        }

    # ---- 综合运行报告 ----

    def pipeline_run_report(
        self,
        pipeline_id: str,
        watermark: WatermarkRecord,
        quality_result: QualityGateResponse | None = None,
    ) -> dict[str, Any]:
        """管线运行综合报告：水位 + 质量 + 告警。"""
        report: dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "timestamp": datetime.now().isoformat(),
        }

        # 水位
        report["watermark"] = {
            "current": watermark.current_watermark,
            "delay_minutes": watermark.delay_minutes,
            "status": watermark.status.value,
        }

        # 质量
        if quality_result:
            report["quality"] = {
                "pass": quality_result.overall_pass,
                "decision": quality_result.decision,
                "failed_ratio": quality_result.failed_ratio,
            }
            report["alerts"] = [
                {"name": a.rule_name, "level": a.level.value, "message": a.message}
                for a in quality_result.triggered_alerts if a.matched
            ]

        # 整体健康评估
        checks = []
        if watermark.status in (WatermarkStatus.DELAYED, WatermarkStatus.SEVERE):
            checks.append("水位延迟")
        if quality_result and not quality_result.overall_pass:
            checks.append("质量门禁不通过")
        if report.get("alerts"):
            checks.append("有未处理告警")

        report["health"] = "healthy" if not checks else "degraded" if len(checks) <= 2 else "critical"
        report["issues"] = checks

        return report