"""Data lineage tracking adapter.

Records source-to-target field mappings, transformation steps, and constructs
a DAG of data flows for impact analysis and audit trails. Emits OpenLineage
events for integration with Marquez, DataHub, or other lineage catalogs.

Lineage graph nodes are datasets (identified by URI), and edges are
transformation jobs. Field-level lineage maps input column paths to output
column paths through each transformation step.
"""

import datetime
import json
import uuid
from collections import defaultdict
from typing import Any

from aumos_common.events import EventPublisher, Topics
from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import DataLineageTrackerProtocol

logger = get_logger(__name__)

# OpenLineage spec namespace
_OPENLINEAGE_PRODUCER = "aumos-data-pipeline"
_OPENLINEAGE_SCHEMA_URL = (
    "https://openlineage.io/spec/1-0-2/OpenLineage.json#/$defs/RunEvent"
)


class DataLineageTracker:
    """Records and queries data lineage for pipeline transformations.

    Implements DataLineageTrackerProtocol. Maintains an in-memory lineage graph
    (directed acyclic graph of dataset → transformation → dataset edges). In
    production this would be persisted to a graph database (Neptune, neo4j) or
    the Marquez API.

    Lineage nodes:
        - Dataset node: {uri, schema, row_count, job_id, tenant_id}

    Lineage edges (job nodes):
        - {job_id, step_name, inputs: [uri], outputs: [uri], field_mappings, timestamp}

    Field mappings:
        - {output_field: [input_field, ...]} within a single transformation step
    """

    def __init__(
        self,
        event_publisher: EventPublisher,
        openlineage_url: str = "",
    ) -> None:
        """Initialize the lineage tracker.

        Args:
            event_publisher: Kafka publisher for lineage events.
            openlineage_url: Optional Marquez/OpenLineage API endpoint (empty to disable HTTP emission).
        """
        self._publisher = event_publisher
        self._openlineage_url = openlineage_url
        # Lineage DAG: {output_uri: [lineage_edge_dict, ...]}
        self._lineage_graph: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # Node registry: {uri: dataset_node_dict}
        self._dataset_nodes: dict[str, dict[str, Any]] = {}

    async def record_transformation(
        self,
        step_name: str,
        input_uris: list[str],
        output_uri: str,
        field_mappings: dict[str, list[str]],
        metadata: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> str:
        """Record a transformation step in the lineage graph.

        Args:
            step_name: Human-readable name of the transformation (e.g. 'clean', 'join').
            input_uris: List of input dataset URIs consumed by this step.
            output_uri: URI of the output dataset produced by this step.
            field_mappings: Output-to-input field mapping: {output_col: [input_cols]}.
            metadata: Additional context (row_count, schema, version, etc.).
            job_id: Pipeline job ID.
            tenant_id: Tenant context.

        Returns:
            Lineage edge ID (UUID string) for the recorded step.
        """
        edge_id = str(uuid.uuid4())
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

        edge: dict[str, Any] = {
            "edge_id": edge_id,
            "step_name": step_name,
            "input_uris": input_uris,
            "output_uri": output_uri,
            "field_mappings": field_mappings,
            "metadata": metadata,
            "job_id": str(job_id),
            "tenant_id": str(tenant_id),
            "timestamp": timestamp,
        }

        # Register output dataset node
        self._dataset_nodes[output_uri] = {
            "uri": output_uri,
            "produced_by_job": str(job_id),
            "tenant_id": str(tenant_id),
            "timestamp": timestamp,
            **{k: v for k, v in metadata.items() if k in ("schema", "row_count", "format")},
        }

        # Register input dataset nodes (if not already registered)
        for input_uri in input_uris:
            if input_uri not in self._dataset_nodes:
                self._dataset_nodes[input_uri] = {
                    "uri": input_uri,
                    "tenant_id": str(tenant_id),
                    "timestamp": timestamp,
                }

        # Add edge to DAG
        self._lineage_graph[output_uri].append(edge)

        logger.info(
            "Lineage step recorded",
            edge_id=edge_id,
            step_name=step_name,
            input_count=len(input_uris),
            output_uri=output_uri,
            field_count=len(field_mappings),
            job_id=str(job_id),
        )

        # Emit OpenLineage event
        await self._emit_openlineage_event(edge)

        return edge_id

    async def trace_field_origin(
        self,
        output_uri: str,
        field_name: str,
        tenant_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        """Trace a specific output field back through the lineage graph to its origins.

        Performs a depth-first traversal of the lineage DAG to find all input
        fields and datasets that contributed to the given output field.

        Args:
            output_uri: URI of the dataset containing the field.
            field_name: Name of the output field to trace.
            tenant_id: Tenant context.

        Returns:
            List of lineage hops: [{step_name, uri, field_name, input_fields, timestamp}]
        """
        trace: list[dict[str, Any]] = []
        self._dfs_trace_field(output_uri, field_name, trace, visited=set())

        logger.info(
            "Field lineage traced",
            output_uri=output_uri,
            field_name=field_name,
            hop_count=len(trace),
            tenant_id=str(tenant_id),
        )
        return trace

    async def get_dataset_lineage(
        self,
        dataset_uri: str,
        tenant_id: uuid.UUID,
        max_depth: int = 10,
    ) -> dict[str, Any]:
        """Retrieve the full lineage graph for a dataset.

        Returns both upstream (provenance) and downstream (impact) lineage.

        Args:
            dataset_uri: URI of the dataset to query.
            tenant_id: Tenant context.
            max_depth: Maximum hops to traverse in each direction.

        Returns:
            Dict with:
                - dataset: Dataset node info
                - upstream: List of upstream transformation edges
                - downstream: List of downstream transformation edges
                - visualization_data: JSON-serializable DAG for rendering
        """
        upstream = self._collect_upstream(dataset_uri, max_depth)
        downstream = self._collect_downstream(dataset_uri, max_depth)

        visualization_data = self._build_visualization_data(
            dataset_uri, upstream, downstream
        )

        return {
            "dataset": self._dataset_nodes.get(dataset_uri, {"uri": dataset_uri}),
            "upstream": upstream,
            "downstream": downstream,
            "visualization_data": visualization_data,
        }

    async def analyze_impact(
        self,
        source_uri: str,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Identify all downstream datasets and fields affected by a source change.

        Args:
            source_uri: URI of the dataset that is changing.
            tenant_id: Tenant context.

        Returns:
            Dict with:
                - affected_datasets: List of downstream dataset URIs
                - affected_fields: Dict of {dataset_uri: [field_names]}
                - impact_depth: Maximum hops from source to furthest affected dataset
        """
        affected_datasets: list[str] = []
        affected_fields: dict[str, list[str]] = defaultdict(list)
        queue: list[tuple[str, int]] = [(source_uri, 0)]
        visited: set[str] = {source_uri}
        max_depth_reached = 0

        while queue:
            current_uri, depth = queue.pop(0)
            max_depth_reached = max(max_depth_reached, depth)

            for output_uri, edges in self._lineage_graph.items():
                for edge in edges:
                    if current_uri in edge["input_uris"] and output_uri not in visited:
                        affected_datasets.append(output_uri)
                        visited.add(output_uri)
                        queue.append((output_uri, depth + 1))

                        # Collect affected fields in the downstream dataset
                        for output_field, input_fields in edge["field_mappings"].items():
                            affected_fields[output_uri].append(output_field)

        logger.info(
            "Impact analysis completed",
            source_uri=source_uri,
            affected_dataset_count=len(affected_datasets),
            impact_depth=max_depth_reached,
            tenant_id=str(tenant_id),
        )

        return {
            "affected_datasets": affected_datasets,
            "affected_fields": dict(affected_fields),
            "impact_depth": max_depth_reached,
        }

    async def export_lineage_graph(
        self,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Export the full lineage graph as a JSON-serializable visualization payload.

        Args:
            tenant_id: Tenant context (used to filter to tenant-owned datasets).

        Returns:
            Dict with nodes (datasets) and edges (transformations) for graph rendering.
        """
        tenant_str = str(tenant_id)

        nodes: list[dict[str, Any]] = [
            {"id": uri, **node}
            for uri, node in self._dataset_nodes.items()
            if node.get("tenant_id") == tenant_str
        ]

        edges: list[dict[str, Any]] = []
        for output_uri, edge_list in self._lineage_graph.items():
            for edge in edge_list:
                if edge.get("tenant_id") != tenant_str:
                    continue
                for input_uri in edge["input_uris"]:
                    edges.append(
                        {
                            "source": input_uri,
                            "target": output_uri,
                            "step_name": edge["step_name"],
                            "edge_id": edge["edge_id"],
                            "job_id": edge["job_id"],
                            "timestamp": edge["timestamp"],
                        }
                    )

        return {
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
        }

    def _dfs_trace_field(
        self,
        current_uri: str,
        field_name: str,
        trace: list[dict[str, Any]],
        visited: set[str],
    ) -> None:
        """Recursively trace a field back through the lineage DAG.

        Args:
            current_uri: Current dataset URI being traced.
            field_name: Field name to look for in input mappings.
            trace: Accumulator list for hop records.
            visited: Set of already-visited URIs to prevent cycles.
        """
        if current_uri in visited:
            return
        visited.add(current_uri)

        edges = self._lineage_graph.get(current_uri, [])
        for edge in edges:
            input_fields = edge["field_mappings"].get(field_name, [])
            if not input_fields:
                continue

            trace.append(
                {
                    "step_name": edge["step_name"],
                    "output_uri": current_uri,
                    "output_field": field_name,
                    "input_uris": edge["input_uris"],
                    "input_fields": input_fields,
                    "timestamp": edge["timestamp"],
                    "job_id": edge["job_id"],
                }
            )

            # Continue tracing each input field in each input dataset
            for input_uri in edge["input_uris"]:
                for input_field in input_fields:
                    self._dfs_trace_field(input_uri, input_field, trace, visited)

    def _collect_upstream(
        self,
        dataset_uri: str,
        max_depth: int,
    ) -> list[dict[str, Any]]:
        """Collect all upstream lineage edges for a dataset.

        Args:
            dataset_uri: Target dataset URI.
            max_depth: Maximum traversal depth.

        Returns:
            List of upstream edge dicts.
        """
        result: list[dict[str, Any]] = []
        queue: list[tuple[str, int]] = [(dataset_uri, 0)]
        visited: set[str] = {dataset_uri}

        while queue:
            current_uri, depth = queue.pop(0)
            if depth >= max_depth:
                continue

            for edge in self._lineage_graph.get(current_uri, []):
                result.append(edge)
                for input_uri in edge["input_uris"]:
                    if input_uri not in visited:
                        visited.add(input_uri)
                        queue.append((input_uri, depth + 1))

        return result

    def _collect_downstream(
        self,
        dataset_uri: str,
        max_depth: int,
    ) -> list[dict[str, Any]]:
        """Collect all downstream lineage edges for a dataset.

        Args:
            dataset_uri: Source dataset URI.
            max_depth: Maximum traversal depth.

        Returns:
            List of downstream edge dicts.
        """
        result: list[dict[str, Any]] = []
        queue: list[tuple[str, int]] = [(dataset_uri, 0)]
        visited: set[str] = {dataset_uri}

        while queue:
            current_uri, depth = queue.pop(0)
            if depth >= max_depth:
                continue

            for output_uri, edges in self._lineage_graph.items():
                for edge in edges:
                    if current_uri in edge["input_uris"] and output_uri not in visited:
                        visited.add(output_uri)
                        result.append(edge)
                        queue.append((output_uri, depth + 1))

        return result

    def _build_visualization_data(
        self,
        root_uri: str,
        upstream: list[dict[str, Any]],
        downstream: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build a JSON-serializable graph for visualization tools.

        Args:
            root_uri: The focal dataset URI.
            upstream: Upstream edges.
            downstream: Downstream edges.

        Returns:
            Graph dict with nodes and edges suitable for D3/Cytoscape rendering.
        """
        node_set: dict[str, dict[str, str]] = {}
        edge_list: list[dict[str, str]] = []

        node_set[root_uri] = {"id": root_uri, "type": "root", "label": root_uri.split("/")[-1]}

        for edge in upstream + downstream:
            for input_uri in edge["input_uris"]:
                if input_uri not in node_set:
                    node_set[input_uri] = {
                        "id": input_uri,
                        "type": "dataset",
                        "label": input_uri.split("/")[-1],
                    }
                edge_list.append(
                    {
                        "source": input_uri,
                        "target": edge["output_uri"],
                        "label": edge["step_name"],
                    }
                )
            if edge["output_uri"] not in node_set:
                node_set[edge["output_uri"]] = {
                    "id": edge["output_uri"],
                    "type": "dataset",
                    "label": edge["output_uri"].split("/")[-1],
                }

        return {
            "nodes": list(node_set.values()),
            "edges": edge_list,
        }

    async def _emit_openlineage_event(self, edge: dict[str, Any]) -> None:
        """Publish an OpenLineage RunEvent to Kafka and optionally to the Marquez API.

        Args:
            edge: Lineage edge dict as stored in the DAG.
        """
        event: dict[str, Any] = {
            "eventType": "COMPLETE",
            "eventTime": edge["timestamp"],
            "producer": _OPENLINEAGE_PRODUCER,
            "schemaURL": _OPENLINEAGE_SCHEMA_URL,
            "run": {
                "runId": edge["edge_id"],
                "facets": {
                    "jobId": edge["job_id"],
                    "tenantId": edge["tenant_id"],
                },
            },
            "job": {
                "namespace": edge["tenant_id"],
                "name": edge["step_name"],
            },
            "inputs": [{"namespace": edge["tenant_id"], "name": uri} for uri in edge["input_uris"]],
            "outputs": [{"namespace": edge["tenant_id"], "name": edge["output_uri"]}],
        }

        try:
            await self._publisher.publish(Topics.PIPELINE_JOB_COMPLETED, event)
        except Exception as exc:
            logger.warning("OpenLineage Kafka emit failed", error=str(exc), edge_id=edge["edge_id"])

        if self._openlineage_url:
            await self._post_openlineage_event(event)

    async def _post_openlineage_event(self, event: dict[str, Any]) -> None:
        """HTTP POST an OpenLineage event to the configured Marquez/catalog endpoint.

        Args:
            event: OpenLineage RunEvent dict.
        """
        try:
            import asyncio
            import urllib.request

            body = json.dumps(event).encode()
            req = urllib.request.Request(
                f"{self._openlineage_url}/api/v1/lineage",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, urllib.request.urlopen, req)
        except Exception as exc:
            logger.warning(
                "OpenLineage HTTP emit failed",
                url=self._openlineage_url,
                error=str(exc),
            )
