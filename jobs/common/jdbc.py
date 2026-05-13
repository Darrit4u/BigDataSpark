from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from common.spark_job import env


@dataclass(frozen=True)
class JdbcConnection:
    host: str
    port: str
    database: str
    user: str
    password: str
    driver: str
    dialect: str = "postgresql"

    @property
    def url(self) -> str:
        return f"jdbc:{self.dialect}://{self.host}:{self.port}/{self.database}"

    @property
    def props(self) -> dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }

    @classmethod
    def postgres_from_env(cls) -> "JdbcConnection":
        return cls(
            host=env("PGHOST", "postgres"),
            port=env("PGPORT", "5432"),
            database=env("PGDATABASE", "lab2"),
            user=env("PGUSER", "user"),
            password=env("PGPASSWORD", "password"),
            driver="org.postgresql.Driver",
            dialect="postgresql",
        )

    @classmethod
    def clickhouse_from_env(cls) -> "JdbcConnection":
        return cls(
            host=env("CLICKHOUSE_HOST", "clickhouse"),
            port=env("CLICKHOUSE_JDBC_PORT", "8123"),
            database=env("CLICKHOUSE_DATABASE", "lab2"),
            user=env("CLICKHOUSE_USER", "user"),
            password=env("CLICKHOUSE_PASSWORD", "password"),
            driver="com.clickhouse.jdbc.ClickHouseDriver",
            dialect="clickhouse",
        )

    def read_table(self, spark: SparkSession, table: str) -> DataFrame:
        return spark.read.jdbc(self.url, table, properties=self.props)

    def read_query(self, spark: SparkSession, query: str, alias: str = "src") -> DataFrame:
        return spark.read.jdbc(self.url, f"({query}) {alias}", properties=self.props)

    def write_table(self, df: DataFrame, table: str, mode: str = "append") -> None:
        (
            df.write.format("jdbc")
            .option("url", self.url)
            .option("dbtable", table)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", self.driver)
            .mode(mode)
            .save()
        )

    def execute_sql(self, spark: SparkSession, statements: list[str]) -> None:
        jvm = spark._sc._jvm
        jvm.Class.forName(self.driver)

        props = jvm.java.util.Properties()
        props.setProperty("user", self.user)
        props.setProperty("password", self.password)

        conn = jvm.java.sql.DriverManager.getConnection(self.url, props)
        try:
            stmt = conn.createStatement()
            try:
                for statement in statements:
                    sql = statement.strip()
                    if not sql:
                        continue
                    stmt.execute(sql)
            finally:
                stmt.close()
        finally:
            conn.close()
