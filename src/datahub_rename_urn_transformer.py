import re
from typing import Dict, Iterable, List

import pydantic

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import (
    MetadataChangeProposalClass,
    MetadataChangeProposalWrapper,
)
from datahub.ingestion.api.common import (
    ControlRecord,
    EndOfStream,
    PipelineContext,
    RecordEnvelope,
)
from datahub.ingestion.api.transform import Transformer

URN_VALIDATION_MESSAGE_TEMPLATE = """
URN must start with `urn:li:`, got %r. (Typical dataset URNs look like `urn:li:dataset:(urn:li:dataPlatform:s3,bucket/mytable,PROD)` or `urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.order_details,PROD)`. If you're trying to understand how to build a particular type of URN, you can pull up an existing entity in the DataHub web UI and see the URN in the URL bar.)
""".strip()


def _extract_regex(regex, value):
    m = re.match(regex, value)
    if not m:
        raise ValueError(f"Couldn't match {repr(regex)} against {repr(value)}")
    return m.group(0)


class RenameSpec(ConfigModel):
    old_urn: str
    new_urn: str

    @pydantic.root_validator(skip_on_failure=True)
    def validate_no_double_stars(cls, values: Dict) -> Dict:
        old_urn = values["old_urn"]
        new_urn = values["new_urn"]

        old_urn_prefix = _extract_regex(r"urn:li:[^:(]+", old_urn)
        new_urn_prefix = _extract_regex(r"urn:li:[^:(]+", new_urn)

        if old_urn_prefix != new_urn_prefix:
            raise ValueError(
                f"old_urn and new_urn must share the same type, got old_urn_type = {repr(old_urn_prefix)} and new_urn_type = {repr(new_urn_prefix)}"
            )

        return values

    @pydantic.validator("old_urn", always=True)
    def check_old_urn(cls, old_urn: str) -> str:
        if not old_urn.startswith("urn:li:"):
            raise ValueError(URN_VALIDATION_MESSAGE_TEMPLATE % old_urn)
        return old_urn

    @pydantic.validator("new_urn", always=True)
    def check_new_urn(cls, new_urn: str) -> str:
        if not new_urn.startswith("urn:li:"):
            raise ValueError(URN_VALIDATION_MESSAGE_TEMPLATE % new_urn)
        return new_urn


# class RenameUrnConfig(TransformerSemanticsConfigModel):
class RenameUrnConfig(ConfigModel):
    rename_specs: List[RenameSpec]


class RenameUrn(Transformer):
    def __init__(self, config: RenameUrnConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx: PipelineContext = ctx
        self.config: RenameUrnConfig = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "RenameUrn":
        config = RenameUrnConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(
                envelope.record,
                (MetadataChangeProposalClass, MetadataChangeProposalWrapper),
            ):
                for spec in self.config.rename_specs:
                    if envelope.record.entityUrn != spec.old_urn:
                        continue

                    # not strictly necessary, but makes ingestion logs make more sense
                    if "workunit_id" in envelope.metadata:
                        envelope.metadata["workunit_id"] = envelope.metadata[
                            "workunit_id"
                        ].replace(envelope.record.entityUrn, spec.new_urn)

                    # change the URN!
                    envelope.record.entityUrn = spec.new_urn

                    break
            elif isinstance(envelope.record, (ControlRecord, EndOfStream)):
                continue
            else:
                raise NotImplementedError(type(envelope.record))
            yield envelope
