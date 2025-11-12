"""
Event Processor
Orchestrates event processing and ingestion
"""

import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Optional

from llama_index.core import Document

from src.services.ingestion.ingestion_service import IngestionService
from src.services.kafka.event_parser import EventParser
from src.services.kafka.models import OperationType
from src.services.security.hmac_service import HMACService
from src.services.security.tenant_secret_manager import TenantSecretManager
from src.utils.logger import get_logger

logger = get_logger("services.kafka.event_processor")


class EventProcessor:
    """Orchestrates event processing and ingestion"""

    def __init__(
        self,
        ingestion_service: IngestionService,
        event_parser: EventParser,
    ):
        """
        Initialize event processor

        Args:
            ingestion_service: IngestionService for document operations
            event_parser: EventParser for Kafka message parsing
        """
        self.ingestion_service = ingestion_service
        self.event_parser = event_parser
        self.tenant_secret_manager = TenantSecretManager()

    async def batch_events_processor(
        self, messages: List[tuple[str, Dict[str, Any], Optional[str]]]
    ) -> None:
        """
        Process multiple messages in a batch with operation-based grouping

        Args:
            messages: List of (topic, message_payload, key) tuples
        """
        if not messages:
            return

        # Parse all documents
        documents: List[Document] = []
        for topic, raw_message, _key in messages:
            message = dict(raw_message)
            provided_signature = message.pop("signature", None)
            try:
                tenant_id = message.get("tenant_id")
                if not tenant_id:
                    logger.error(
                        "Missing tenant_id in message for topic=%s event_id=%s",
                        topic,
                        message.get("event_id"),
                    )
                    continue

                if not await self._verify_event_signature(
                    tenant_id=tenant_id,
                    payload=message,
                    provided_signature=provided_signature,
                ):
                    continue

                document = self.event_parser.parse(topic, message)
                documents.append(document)
            except Exception as e:
                logger.error(
                    f"Error building document for topic={topic}, "
                    f"tenant={message.get('tenant_id')}: {e}",
                    exc_info=True,
                )
                # Continue processing other messages in batch

        if not documents:
            logger.warning("No valid documents to process in batch")
            return

        operations_by_tenant: Dict[str, Dict[str, List[Document]]] = defaultdict(
            lambda: {"all": [], "create": [], "update": [], "delete": []}
        )
        for document in documents:
            tenant_id = document.metadata.get("tenant_id")
            if not tenant_id:
                logger.error("Document missing tenant_id metadata: id=%s", document.id_)
                continue

            operations_by_tenant[tenant_id]["all"].append(document)
            operation = document.metadata.get("operation")
            if operation == OperationType.CREATE.value:
                operations_by_tenant[tenant_id]["create"].append(document)
            elif operation == OperationType.UPDATE.value:
                operations_by_tenant[tenant_id]["update"].append(document)
            elif operation == OperationType.DELETE.value:
                operations_by_tenant[tenant_id]["delete"].append(document)

        logger.info(
            "\n----------------- Batch Processing ----------------"
            "\nTotal Documents: %s"
            "\nTenants: %s"
            "\n---------------------------------------------------",
            len(documents),
            list(operations_by_tenant.keys()),
        )

        try:
            for tenant_id, buckets in operations_by_tenant.items():
                create_docs = buckets["create"]
                update_docs = buckets["update"]
                delete_docs = buckets["delete"]

                logger.info(
                    "Tenant %s batch summary: total=%s create=%s update=%s delete=%s",
                    tenant_id,
                    len(buckets["all"]),
                    len(create_docs),
                    len(update_docs),
                    len(delete_docs),
                )

                if create_docs:
                    await self.ingestion_service.add_documents(
                        create_docs, tenant_id=tenant_id
                    )

                if delete_docs:
                    await self.ingestion_service.delete_documents(
                        tenant_id=tenant_id,
                        ids=[document.id_ for document in delete_docs],
                    )

                if update_docs:
                    try:
                        await self.ingestion_service.delete_documents(
                            tenant_id=tenant_id,
                            ids=[document.id_ for document in update_docs],
                        )
                        await asyncio.sleep(0.3)
                        await self.ingestion_service.add_documents(
                            update_docs, tenant_id=tenant_id
                        )
                    except Exception as e:
                        logger.error(
                            "UPDATE operation failed for tenant %s: %s",
                            tenant_id,
                            e,
                            exc_info=True,
                        )
                        raise

            logger.info(
                "\n----------------- Batch Processed Successfully ----------------"
                "\nTotal Documents: %s"
                "\n---------------------------------------------------------------",
                len(documents),
            )
        except Exception as e:
            logger.error(f"Error ingesting batch: {e}", exc_info=True)
            raise

    async def _verify_event_signature(
        self,
        tenant_id: str,
        payload: Dict[str, Any],
        provided_signature: Optional[str],
    ) -> bool:
        if not provided_signature:
            logger.error(
                "Missing HMAC signature for tenant=%s event_id=%s",
                tenant_id,
                payload.get("event_id"),
            )
            return False

        try:
            secrets = await self.tenant_secret_manager.get_secrets(tenant_id)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(
                "Failed to load secrets for tenant=%s: %s",
                tenant_id,
                exc,
                exc_info=True,
            )
            return False

        is_valid, key_used = HMACService.verify_signature_with_fallback(
            primary_key=secrets.primary,
            secondary_key=secrets.secondary,
            payload=payload,
            provided_signature=provided_signature,
        )

        if not is_valid:
            logger.error(
                "HMAC verification failed for tenant=%s event_id=%s (key_used=%s)",
                tenant_id,
                payload.get("event_id"),
                key_used,
            )
        return is_valid
