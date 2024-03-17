from typing import List

from lib.pg import PgConnect
from stg_loader.repository.stg_models import StgModel


class StgRepository:
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

    def insert(self, model: StgModel) -> str:
        query = self._query_insert(table_name=model.table_name,
                                   lst_columns=list(model.field_names()),
                                   unique_keys=model.unique_columns
                                   )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, model.dict())

        return query
