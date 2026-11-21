"""质量规则校验执行引擎。

校验类型：NOT_NULL / UNIQUE / REFERENCE / EXPRESSION / RANGE_VALUE / PATTERN_MATCH
输入：规则定义 + 行级数据抽样 → 输出：通过/失败/阻断决策。

分区质检（partition gate）在规则执行前先校验采集覆盖，每个分区的样本执行数据
必须能区分三种采集状态：

- ``CollectionStatus.COLLECTED``：已采集，``rows=[]`` 表示真实零行（合法空分区），
  不补造分母，规则在零行上的坏行数为 0；
- ``CollectionStatus.NOT_COLLECTED``：上游尚未回报，分区没有有效质检结论，
  整批门禁不能判定通过，也不计入任何坏行；
- ``CollectionStatus.COLLECTION_FAILED``：采集凭据缺失/采集失败，同样没有有效结论，
  恢复后重试时仍使用实际到齐的样本与规则原阈值。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class CheckType(str, Enum):
    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    REFERENCE = "REFERENCE"
    EXPRESSION = "EXPRESSION"
    RANGE_VALUE = "RANGE_VALUE"
    PATTERN_MATCH = "PATTERN_MATCH"


class CheckLevel(str, Enum):
    """校验级别决定失败后的处理方式。"""
    WARN = "WARN"       # 仅告警
    BLOCK = "BLOCK"     # 阻断管线
    FIX = "FIX"         # 自动修复尝试


class CollectionStatus(str, Enum):
    """分区样本的采集状态。"""
    COLLECTED = "COLLECTED"                    # 已采集（含真实零行的空分区）
    NOT_COLLECTED = "NOT_COLLECTED"            # 上游尚未回报，待恢复
    COLLECTION_FAILED = "COLLECTION_FAILED"    # 采集凭据缺失/采集失败

    @property
    def label(self) -> str:
        return {
            CollectionStatus.COLLECTED: "已采集",
            CollectionStatus.NOT_COLLECTED: "尚未采集",
            CollectionStatus.COLLECTION_FAILED: "采集失败",
        }[self]

    @property
    def has_valid_conclusion(self) -> bool:
        """只有真正到齐样本的分区才有有效质检结论。"""
        return self is CollectionStatus.COLLECTED


@dataclass
class PartitionSample:
    """一个分区的样本执行数据。

    ``rows`` 只在 ``status=COLLECTED`` 时有意义；空列表表示真实零行，
    与 NOT_COLLECTED / COLLECTION_FAILED 严格区分，不得互相冒充。
    """
    partition: str
    status: CollectionStatus = CollectionStatus.COLLECTED
    rows: list[dict[str, Any]] = field(default_factory=list)
    error_message: str = ""

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "PartitionSample":
        raw_status = str(payload.get("status", CollectionStatus.COLLECTED.value)).strip()
        try:
            status = CollectionStatus(raw_status)
        except ValueError:
            # 兼容中文状态与未知取值：未知状态按尚未采集处理，避免误判为已采集
            alias = {
                "已采集": CollectionStatus.COLLECTED,
                "真实零行": CollectionStatus.COLLECTED,
                "尚未采集": CollectionStatus.NOT_COLLECTED,
                "待采集": CollectionStatus.NOT_COLLECTED,
                "未回报": CollectionStatus.NOT_COLLECTED,
                "采集失败": CollectionStatus.COLLECTION_FAILED,
                "凭据缺失": CollectionStatus.COLLECTION_FAILED,
            }
            status = alias.get(raw_status, CollectionStatus.NOT_COLLECTED)
        rows = payload.get("rows") or []
        return cls(
            partition=str(payload.get("partition", "")).strip(),
            status=status,
            rows=list(rows),
            error_message=str(payload.get("error_message", "") or ""),
        )

    @property
    def sample_count(self) -> int:
        """实际到齐的样本行数；未到齐/失败的分区不提供分母。"""
        return len(self.rows) if self.status is CollectionStatus.COLLECTED else 0


@dataclass
class QualityRule:
    """一条质量校验规则定义。"""
    rule_id: str
    rule_name: str
    target_table: str
    check_field: str
    check_type: CheckType
    threshold: float = 0.0        # 允许的最大失败比例 (0~1)
    level: CheckLevel = CheckLevel.WARN
    extra: dict[str, Any] = field(default_factory=dict)
    # extra 可包含：
    #   reference_table, reference_field → REFERENCE
    #   expression → EXPRESSION (Python表达式，字段值通过 {value} 传入)
    #   pattern → PATTERN_MATCH (正则)
    #   min_value, max_value → RANGE_VALUE


@dataclass
class ValidationResult:
    """单条规则对一批数据的校验结果。"""
    rule_id: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    failed_ratio: float
    threshold_exceeded: bool
    blocked: bool  # 是否触发阻断
    error_samples: list[dict[str, Any]] = field(default_factory=list)
    # 样本是否真实到齐：空样本（缺采集凭据/未采集）时为 False，
    # 此时不产生有效质检结论，调用方不得把零行当成通过。
    samples_collected: bool = True
    # 真实零行：样本到齐但确实一行都没有，分母为 0 而不是补造的 1。
    genuine_empty: bool = False


class ValidationEngine:
    """质量规则校验引擎。

    用法:
        engine = ValidationEngine()
        rule = QualityRule(rule_id="r1", rule_name="非空检查",
                           target_table="dw.dwd_order", check_field="order_id",
                           check_type=CheckType.NOT_NULL, threshold=0.01)
        result = engine.run(rule, [{"order_id": "123"}, {"order_id": None}])
    """

    _EXPR_BUILTINS: dict[str, Any] = {
        "len": len, "abs": abs, "min": min, "max": max,
        "round": round, "int": int, "float": float, "str": str,
    }

    def __init__(self) -> None:
        self._checkers: dict[CheckType, Callable[..., tuple[bool, str | None]]] = {
            CheckType.NOT_NULL: self._check_not_null,
            CheckType.UNIQUE: self._check_unique,
            CheckType.REFERENCE: self._check_reference,
            CheckType.EXPRESSION: self._check_expression,
            CheckType.RANGE_VALUE: self._check_range,
            CheckType.PATTERN_MATCH: self._check_pattern,
        }
        # 引用数据集（模拟引用完整性校验的查找表）
        self._reference_sets: dict[str, set[Any]] = {}

    def set_reference(self, ref_key: str, values: set[Any]) -> None:
        """预置引用查找集。"""
        self._reference_sets[ref_key] = values

    def run(
        self,
        rule: QualityRule,
        rows: list[dict[str, Any]],
        *,
        samples_collected: bool = True,
    ) -> ValidationResult:
        """执行单条规则。

        ``samples_collected=False`` 表示样本未真实到齐（缺失采集凭据、尚未采集
        或采集失败）：不产生有效质检结论，不补造任何分母，也不输出伪样本行。
        ``rows=[]`` 且 ``samples_collected=True`` 表示真实零行：合法通过，
        ``total_rows`` 保留 0，坏行率为 0。
        """
        if not samples_collected:
            return ValidationResult(
                rule_id=rule.rule_id,
                total_rows=0, passed_rows=0, failed_rows=0,
                failed_ratio=0.0, threshold_exceeded=False, blocked=False,
                error_samples=[],
                samples_collected=False, genuine_empty=False,
            )

        checker = self._checkers.get(rule.check_type)
        if checker is None:
            return ValidationResult(
                rule_id=rule.rule_id,
                total_rows=len(rows), passed_rows=0, failed_rows=len(rows),
                failed_ratio=1.0 if rows else 0.0,
                threshold_exceeded=bool(rows), blocked=bool(rows),
                error_samples=[{"error": f"未知校验类型 {rule.check_type.value}"}] if rows else [],
                samples_collected=True, genuine_empty=not rows,
            )

        if rule.check_type == CheckType.UNIQUE:
            passed, failed, samples = checker(rows, rule)
        else:
            passed, failed, samples = checker(rows, rule)

        total = len(rows)
        # 真实零行：分母为 0 而不是补造的 1，坏行率按 0 处理。
        ratio = failed / total if total else 0.0
        blocked = rule.level == CheckLevel.BLOCK and ratio > rule.threshold
        return ValidationResult(
            rule_id=rule.rule_id,
            total_rows=total,
            passed_rows=passed,
            failed_rows=failed,
            failed_ratio=ratio,
            threshold_exceeded=ratio > rule.threshold,
            blocked=blocked,
            error_samples=samples[:5],
            samples_collected=True,
            genuine_empty=(total == 0),
        )

    # ---- 内置校验器 ----

    @staticmethod
    def _check_not_null(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            if val is not None and str(val).strip() != "":
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({"row_index": i, rule.check_field: val, "reason": "NULL/空值"})
        return passed, failed, samples

    @staticmethod
    def _check_unique(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        seen: set[Any] = set()
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            if val is not None and val not in seen:
                seen.add(val)
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": "重复值" if val in seen else "NULL值",
                    })
        return passed, failed, samples

    def _check_reference(
        self, rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        ref_key = rule.extra.get("reference_table", "")
        ref_set = self._reference_sets.get(ref_key, set())
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            if val is None:
                failed += 1
                if len(samples) < 5:
                    samples.append({"row_index": i, rule.check_field: val, "reason": "NULL值"})
            elif val in ref_set:
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": f"引用缺失: {val} 不在 {ref_key} 中",
                    })
        return passed, failed, samples

    @staticmethod
    def _check_expression(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        expr_src = rule.extra.get("expression", "True")
        # 安全编译表达式
        try:
            expr = compile(expr_src, "<quality_expression>", "eval")
        except SyntaxError:
            return 0, len(rows), [{"error": f"表达式语法错误: {expr_src}"}]

        namespace: dict[str, Any] = {
            "__builtins__": None,
        }
        namespace.update({
            "len": len, "abs": abs, "min": min, "max": max,
            "round": round, "int": int, "float": float, "str": str,
            "re": re,
        })

        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            local = dict(namespace)
            local["value"] = val
            for k, v in row.items():
                if k not in local:
                    local[k] = v
            try:
                ok = bool(eval(expr, {"__builtins__": {}}, local))
            except Exception:
                ok = False
            if ok:
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": f"表达式不满足: {expr_src}",
                    })
        return passed, failed, samples

    @staticmethod
    def _check_range(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        min_val = rule.extra.get("min_value")
        max_val = rule.extra.get("max_value")
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = row.get(rule.check_field)
            try:
                num = float(val) if val is not None else None
            except (TypeError, ValueError):
                num = None
            ok = True
            reason = ""
            if num is None:
                ok = False
                reason = "NULL/非数字"
            elif min_val is not None and num < float(min_val):
                ok = False
                reason = f"小于下限 {min_val}"
            elif max_val is not None and num > float(max_val):
                ok = False
                reason = f"大于上限 {max_val}"
            if ok:
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val, "reason": reason,
                    })
        return passed, failed, samples

    @staticmethod
    def _check_pattern(
        rows: list[dict[str, Any]], rule: QualityRule
    ) -> tuple[int, int, list[dict[str, Any]]]:
        pattern = rule.extra.get("pattern", ".*")
        try:
            compiled = re.compile(pattern)
        except re.error:
            return 0, len(rows), [{"error": f"正则错误: {pattern}"}]
        passed = 0
        failed = 0
        samples: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            val = str(row.get(rule.check_field, ""))
            if compiled.match(val):
                passed += 1
            else:
                failed += 1
                if len(samples) < 5:
                    samples.append({
                        "row_index": i, rule.check_field: val,
                        "reason": f"不匹配模式 {pattern}",
                    })
        return passed, failed, samples

    # ---- 批量执行 ----

    def run_batch(
        self,
        rules: list[QualityRule],
        rows: list[dict[str, Any]],
        *,
        samples_collected: bool = True,
    ) -> list[ValidationResult]:
        """对同一批数据执行多条规则，返回结果列表。"""
        return [
            self.run(rule, rows, samples_collected=samples_collected)
            for rule in rules
        ]

    def run_pipeline_quality(
        self,
        rules: list[QualityRule],
        rows: list[dict[str, Any]],
        pipeline_id: str,
        *,
        samples_collected: bool = True,
    ) -> dict[str, Any]:
        """完整的管线质量门禁：执行所有规则 → 汇总 → 通过/阻断决策。

        ``samples_collected=False`` 表示入口收到的是缺失采集凭据的空样本：
        没有有效质检结论，门禁不得通过，且校验详情不补造任何样本行。
        """
        results = self.run_batch(rules, rows, samples_collected=samples_collected)
        if not samples_collected:
            any_blocked = False
            total_failed = 0
            total_rows = 0
            decision = "PENDING_COLLECTION"
        else:
            any_blocked = any(r.blocked for r in results)
            total_failed = sum(r.failed_rows for r in results)
            total_rows = sum(r.total_rows for r in results)
            decision = "RELEASE" if not any_blocked else "BLOCKED"
        # 真实零行时 total_rows=0，坏行率为 0，绝不补造分母。
        overall_ratio = round(total_failed / total_rows, 4) if total_rows else 0.0
        return {
            "pipeline_id": pipeline_id,
            "samples_collected": samples_collected,
            "genuine_empty": samples_collected and total_rows == 0,
            "rule_results": [
                {
                    "rule_id": r.rule_id,
                    "total": r.total_rows,
                    "passed": r.passed_rows,
                    "failed": r.failed_rows,
                    "ratio": round(r.failed_ratio, 4),
                    "blocked": r.blocked,
                    "samples_collected": r.samples_collected,
                    "genuine_empty": r.genuine_empty,
                    # 真实零行或未到齐时没有坏行样本，详情里不会多出伪造的一行
                    "error_samples": r.error_samples,
                }
                for r in results
            ],
            "overall_pass": samples_collected and not any_blocked,
            "overall_failed_ratio": overall_ratio,
            "decision": decision,
        }

    # ---- 分区质检 ----

    def run_partition_gate(
        self,
        rules: list[QualityRule],
        partitions: list[PartitionSample],
        pipeline_id: str,
    ) -> dict[str, Any]:
        """分区质检：先校验采集覆盖，再按实际到齐的样本逐规则统计坏行。

        决策规则：

        - 任一分区 NOT_COLLECTED / COLLECTION_FAILED → 该分区无有效结论，
          整批 ``overall_pass=False``，decision=``PENDING_COLLECTION``（若同时
          已有坏行阻断则为 ``BLOCKED``，值班人员仍可先处理坏行）；
        - 全部到齐且无规则超阈阻断 → ``RELEASE``；
        - 已到齐分区上存在阻断规则超阈 → ``BLOCKED``。

        真实零行分区参与覆盖计数但不贡献分母；未到齐分区既不贡献分母也不
        贡献坏行，恢复后重试仍按实际到齐样本与规则原阈值重新计算。
        """
        partition_details: list[dict[str, Any]] = []
        # rule_id -> 跨已到齐分区的聚合坏行统计
        agg: dict[str, dict[str, Any]] = {
            rule.rule_id: {
                "rule": rule,
                "total": 0,
                "passed": 0,
                "failed": 0,
                "error_samples": [],
                "bad_partitions": [],
            }
            for rule in rules
        }

        collected_count = 0
        genuine_empty_count = 0
        pending_partitions: list[dict[str, Any]] = []
        failed_partitions: list[dict[str, Any]] = []
        total_sample_rows = 0

        for sample in partitions:
            is_collected = sample.status is CollectionStatus.COLLECTED
            detail: dict[str, Any] = {
                "partition": sample.partition,
                "collection_status": sample.status.value,
                "collection_status_label": sample.status.label,
                "has_valid_conclusion": sample.status.has_valid_conclusion,
                "sample_rows": sample.sample_count,
                "genuine_empty": is_collected and sample.sample_count == 0,
                "rule_results": [],
                "error_message": sample.error_message,
            }

            if not is_collected:
                target = (
                    failed_partitions
                    if sample.status is CollectionStatus.COLLECTION_FAILED
                    else pending_partitions
                )
                target.append({
                    "partition": sample.partition,
                    "status": sample.status.value,
                    "status_label": sample.status.label,
                    "error_message": sample.error_message,
                })
                partition_details.append(detail)
                continue

            collected_count += 1
            total_sample_rows += sample.sample_count
            if sample.sample_count == 0:
                genuine_empty_count += 1

            for rule in rules:
                result = self.run(rule, sample.rows)
                bucket = agg[rule.rule_id]
                bucket["total"] += result.total_rows
                bucket["passed"] += result.passed_rows
                bucket["failed"] += result.failed_rows
                for err in result.error_samples:
                    tagged = {"partition": sample.partition, **err}
                    if len(bucket["error_samples"]) < 10:
                        bucket["error_samples"].append(tagged)
                if result.failed_rows > 0:
                    bucket["bad_partitions"].append(sample.partition)
                detail["rule_results"].append({
                    "rule_id": rule.rule_id,
                    "total": result.total_rows,
                    "passed": result.passed_rows,
                    "failed": result.failed_rows,
                    "ratio": round(result.failed_ratio, 4),
                    "blocked": result.blocked,
                    "genuine_empty": result.genuine_empty,
                    "error_samples": result.error_samples,
                })

            partition_details.append(detail)

        # 逐规则汇总：阈值保持规则原值，恢复重试不改变阈值
        rule_summaries: list[dict[str, Any]] = []
        any_blocked = False
        total_bad_rows = 0
        for rule in rules:
            bucket = agg[rule.rule_id]
            total = bucket["total"]
            failed = bucket["failed"]
            ratio = failed / total if total else 0.0
            threshold_exceeded = ratio > rule.threshold
            blocked = rule.level == CheckLevel.BLOCK and threshold_exceeded
            any_blocked = any_blocked or blocked
            total_bad_rows += failed
            rule_summaries.append({
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "threshold": rule.threshold,
                "level": rule.level.value,
                "total_rows": total,
                "passed_rows": bucket["passed"],
                "failed_rows": failed,
                "failed_ratio": round(ratio, 4),
                "threshold_exceeded": threshold_exceeded,
                "blocked": blocked,
                "bad_partitions": sorted(set(bucket["bad_partitions"])),
                # 已确认为坏行的订单记录（带分区定位），与待恢复分区严格分开
                "bad_row_samples": bucket["error_samples"],
            })

        total_partitions = len(partitions)
        pending_count = len(pending_partitions) + len(failed_partitions)
        # 一个分区都没提交等同缺失采集凭据的空样本：没有任何有效结论，
        # 不能视为覆盖完整，更不能判通过。
        coverage_complete = total_partitions > 0 and pending_count == 0
        coverage_pct = round(collected_count / total_partitions, 4) if total_partitions else 0.0

        if not coverage_complete:
            # 采集未齐：缺失分区没有有效质检结论，整批不能算通过；
            # 若已到齐样本里已有阻断级坏行，优先提示 BLOCKED，否则等待采集恢复。
            decision = "BLOCKED" if any_blocked else "PENDING_COLLECTION"
        else:
            decision = "BLOCKED" if any_blocked else "RELEASE"

        return {
            "pipeline_id": pipeline_id,
            "overall_pass": coverage_complete and not any_blocked,
            "decision": decision,
            "coverage": {
                "total_partitions": total_partitions,
                "collected_partitions": collected_count,
                "genuine_empty_partitions": genuine_empty_count,
                "pending_partitions": pending_count,
                "not_collected_partitions": [p["partition"] for p in pending_partitions],
                "collection_failed_partitions": [p["partition"] for p in failed_partitions],
                "coverage_pct": coverage_pct,
                "coverage_complete": coverage_complete,
            },
            # 实际覆盖到的样本量（真实零行不补造分母）
            "total_sample_rows": total_sample_rows,
            "total_bad_rows": total_bad_rows,
            "overall_failed_ratio": (
                round(total_bad_rows / total_sample_rows, 4)
                if total_sample_rows else 0.0
            ),
            "rule_results": rule_summaries,
            "partition_results": partition_details,
            # 采集待恢复记录：值班人员需等待这些分区回报后重试
            "pending_collection": pending_partitions,
            "collection_failures": failed_partitions,
        }