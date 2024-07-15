import pandas as pd
from os.path import join
from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyiceberg import Table, Schema, types
from pyarrow import csv
from sqlalchemy import text
from fsspec.implementations.local import LocalFileSystem

class IcebergHandler(DbTypeHandler):
    def __init__(self, fs_config=None):
        self.fs = LocalFileSystem(**(fs_config or {}))  # 使用LocalFileSystem，您可以根据需要更改为其他类型的文件系统

    def handle_output(self, context: OutputContext, table_slice: TableSlice, df: pd.DataFrame, connection):
        local_path = join("target", table_slice.schema, table_slice.table)
        self.fs.makedirs(local_path, exist_ok=True)

        # 定义Iceberg的schema
        schema = Schema([
            # 根据df或您的数据模型定义schema字段
        ])
        
        # 使用pyiceberg创建表
        table = Table.for_all_data(self.fs, local_path, schema=schema)

        # 将DataFrame写入Iceberg表
        with table.new_row() as row:
            for _, record in df.iterrows():
                row.append(*record)

        context.add_output_metadata({
            "local_path": local_path
        })

        # 注册表的SQL操作
        try:
            with connection as conn:
                conn.execute(text(f"""
                    CALL iceberg.system.register_table(
                        schema_name => :schema_name, 
                        table_name => :table_name, 
                        table_location => :table_location
                    )
                """), {
                    'schema_name': table_slice.schema,
                    'table_name': table_slice.table,
                    'table_location': local_path
                })
                conn.commit()
        except Exception as e:
            context.log.error(f"Error registering table: {e}")

    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        # 读取Iceberg表
        table = Table(self.fs, join("target", table_slice.schema, table_slice.table))
        # 读取表到DataFrame
        df = table.to_pandas()
        return df

    @property
    def supported_types(self):
        return [pd.DataFrame]

    @property
    def requires_fsspec(self):
        return True
