from typing import Iterable

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
new_urn must be a URN that starts with `urn:li:`, got %r. (Typical dataset URNs look like `urn:li:dataset:(urn:li:dataPlatform:s3,bucket/mytable,PROD)` or `urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.order_details,PROD)`. If you're trying to understand how to build a particular type of URN, you can pull up an existing entity in the DataHub web UI and see the URN in the URL bar.)
""".strip()


# class RenameUrnConfig(TransformerSemanticsConfigModel):
class RenameUrnConfig(ConfigModel):
    new_urn: str

    @pydantic.validator("new_urn", always=True)
    def check_new_urn(cls, new_urn: str) -> str:
        if not new_urn.startswith("urn:li:"):
            raise ValueError(URN_VALIDATION_MESSAGE_TEMPLATE % new_urn)
        return new_urn


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
                # not strictly necessary, but makes ingestion logs make more sense
                if "workunit_id" in envelope.metadata:
                    envelope.metadata["workunit_id"] = envelope.metadata[
                        "workunit_id"
                    ].replace(envelope.record.entityUrn, self.config.new_urn)

                # change the URN!
                envelope.record.entityUrn = self.config.new_urn
            elif isinstance(envelope.record, (ControlRecord, EndOfStream)):
                continue
            else:
                raise NotImplementedError(type(envelope.record))
            yield envelope
