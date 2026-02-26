"""Feature engineering and format conversion adapter.

Implements TransformerProtocol with support for:
- Feature scaling (min-max, z-score)
- Categorical encoding
- Feature interactions (polynomial, cross products)
- Dimensionality reduction (PCA)
- Column dropping and renaming
- Format conversion (CSV → Parquet, etc.)
"""

import io
import uuid
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler, PolynomialFeatures, StandardScaler

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import TransformerProtocol

logger = get_logger(__name__)


class FeatureTransformer:
    """Applies feature engineering and format conversion to DataFrames.

    Implements TransformerProtocol. Operations execute in order:
    drop → rename → scale → encode → interactions → pca → target format conversion.
    """

    async def transform(
        self,
        dataframe: pd.DataFrame,
        transform_config: dict[str, Any],
    ) -> pd.DataFrame:
        """Apply feature engineering transformations.

        Args:
            dataframe: Cleaned dataset to transform.
            transform_config: Transformation parameters:
                - drop_cols: List of columns to drop
                - rename: {old_name: new_name} map
                - scale: {col: 'minmax' | 'zscore'}
                - encode: {col: 'onehot' | 'label'}
                - interactions: List of [col1, col2] pairs for cross products
                - polynomial_cols: Columns for polynomial feature expansion (degree=2)
                - pca: {cols: [...], n_components: int} for PCA reduction

        Returns:
            Transformed DataFrame.
        """
        df = dataframe.copy()
        rows = len(df)
        cols_before = len(df.columns)

        logger.info("Transformation started", rows=rows, columns=cols_before)

        # Step 1: Drop columns
        drop_cols: list[str] = transform_config.get("drop_cols", [])
        if drop_cols:
            existing = [c for c in drop_cols if c in df.columns]
            df = df.drop(columns=existing)

        # Step 2: Rename columns
        rename_map: dict[str, str] = transform_config.get("rename", {})
        if rename_map:
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # Step 3: Scale numeric columns
        scale_config: dict[str, str] = transform_config.get("scale", {})
        for col, method in scale_config.items():
            if col not in df.columns:
                continue
            if not pd.api.types.is_numeric_dtype(df[col]):
                logger.warning("Scaling skipped — non-numeric column", column=col)
                continue
            values = df[[col]].values
            scaler = MinMaxScaler() if method == "minmax" else StandardScaler()
            df[col] = scaler.fit_transform(values).flatten()

        # Step 4: Encode categoricals
        encode_config: dict[str, str] = transform_config.get("encode", {})
        for col, enc_type in encode_config.items():
            if col not in df.columns:
                continue
            if enc_type == "onehot":
                dummies = pd.get_dummies(df[col], prefix=col, drop_first=False)
                df = pd.concat([df.drop(columns=[col]), dummies], axis=1)
            elif enc_type == "label":
                from sklearn.preprocessing import LabelEncoder  # noqa: PLC0415
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))

        # Step 5: Feature interactions (cross products)
        interactions: list[list[str]] = transform_config.get("interactions", [])
        for pair in interactions:
            if len(pair) == 2 and pair[0] in df.columns and pair[1] in df.columns:
                col_a, col_b = pair[0], pair[1]
                if pd.api.types.is_numeric_dtype(df[col_a]) and pd.api.types.is_numeric_dtype(df[col_b]):
                    df[f"{col_a}_x_{col_b}"] = df[col_a] * df[col_b]

        # Step 6: Polynomial features
        poly_cols: list[str] = transform_config.get("polynomial_cols", [])
        if poly_cols:
            valid_poly = [c for c in poly_cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
            if valid_poly:
                poly = PolynomialFeatures(degree=2, include_bias=False, interaction_only=False)
                poly_input = df[valid_poly].values
                poly_output = poly.fit_transform(poly_input)
                poly_feature_names = poly.get_feature_names_out(valid_poly)
                # Add only the new interaction/polynomial columns (not the originals)
                new_col_names = [n for n in poly_feature_names if n not in valid_poly]
                new_col_indices = [i for i, n in enumerate(poly_feature_names) if n not in valid_poly]
                for idx, name in zip(new_col_indices, new_col_names, strict=True):
                    df[name] = poly_output[:, idx]

        # Step 7: PCA dimensionality reduction
        pca_config: dict[str, Any] = transform_config.get("pca", {})
        if pca_config:
            pca_cols: list[str] = pca_config.get("cols", [])
            n_components: int = pca_config.get("n_components", 2)
            valid_pca = [c for c in pca_cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
            if len(valid_pca) >= n_components:
                pca = PCA(n_components=n_components)
                pca_output = pca.fit_transform(df[valid_pca].fillna(0).values)
                df = df.drop(columns=valid_pca)
                for i in range(n_components):
                    df[f"pca_{i + 1}"] = pca_output[:, i]

        logger.info(
            "Transformation completed",
            rows=len(df),
            columns_before=cols_before,
            columns_after=len(df.columns),
        )
        return df

    async def convert_format(
        self,
        input_uri: str,
        output_uri: str,
        target_format: str,
        compression: str = "snappy",
    ) -> str:
        """Convert a dataset between file formats.

        Args:
            input_uri: Source file URI.
            output_uri: Destination file URI.
            target_format: Target format ('parquet', 'arrow', 'csv', 'json').
            compression: Compression codec for parquet output.

        Returns:
            output_uri (unchanged — caller stores the file separately).
        """
        # This is a stub — actual I/O goes through the storage adapter.
        # The storage adapter handles reading/writing to MinIO; this method
        # handles the in-memory format conversion logic.
        logger.info(
            "Format conversion",
            input_uri=input_uri,
            output_uri=output_uri,
            target_format=target_format,
        )
        return output_uri

    def dataframe_to_parquet_bytes(
        self,
        dataframe: pd.DataFrame,
        compression: str = "snappy",
    ) -> bytes:
        """Serialize a DataFrame to Parquet bytes.

        Args:
            dataframe: DataFrame to serialize.
            compression: Parquet compression codec.

        Returns:
            Parquet-encoded bytes.
        """
        table = pa.Table.from_pandas(dataframe)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression=compression)
        return buf.getvalue()

    def dataframe_to_arrow_bytes(self, dataframe: pd.DataFrame) -> bytes:
        """Serialize a DataFrame to Arrow IPC bytes.

        Args:
            dataframe: DataFrame to serialize.

        Returns:
            Arrow IPC-encoded bytes.
        """
        table = pa.Table.from_pandas(dataframe)
        buf = io.BytesIO()
        writer = pa.ipc.new_file(buf, table.schema)
        writer.write_table(table)
        writer.close()
        return buf.getvalue()
