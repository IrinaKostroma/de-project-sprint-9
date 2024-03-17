from typing import List
from uuid import UUID

from lib.pg import PgConnect
from dds_loader.repository.dds_models import DdsModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def _query_insert(self,
                      table_name: str,
                      lst_columns: List[str],
                      unique_keys: List[str]) -> str:
        columns = ', '.join(lst_columns)
        values = ', '.join([f"%({name})s" for name in lst_columns])
        uniques = ', '.join(unique_keys)
        updates = ', '.join([f"{name}=excluded.{name}" for name in lst_columns])

        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({values})
        ON CONFLICT ({uniques})
        DO UPDATE SET {updates};
        """
        return query

    def insert(self, model: DdsModel) -> None:
        query = self._query_insert(table_name=model.table_name,
                                   lst_columns=model.field_names(),
                                   unique_keys=model.unique_columns
                                   )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, model.dict())

    def get_data(self, h_user_pk: UUID, column_lst: List[str]) -> list:
        columns = ', '.join(column_lst)
        query = f"""
        WITH t AS 
            (SELECT lou.h_user_pk,
                hp.h_product_pk,
                hc.h_category_pk,
                spn."name",
                hc.category_name
            FROM dds.h_product hp 
            JOIN dds.s_product_names spn ON hp.h_product_pk = spn.h_product_pk
            JOIN dds.l_product_category lpc ON lpc.h_product_pk = hp.h_product_pk 
            JOIN dds.h_category hc ON lpc.h_category_pk = hc.h_category_pk
            JOIN dds.l_order_product lop ON lop.h_product_pk = hp.h_product_pk
            JOIN dds.h_order ho ON ho.h_order_pk = lop.h_order_pk
            JOIN dds.l_order_user lou ON lou.h_order_pk = ho.h_order_pk
            )
        SELECT h_user_pk, {columns}, COUNT(*) 
        FROM t
        WHERE h_user_pk = '{h_user_pk}'
        GROUP BY h_user_pk, {columns};
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()

        return result
