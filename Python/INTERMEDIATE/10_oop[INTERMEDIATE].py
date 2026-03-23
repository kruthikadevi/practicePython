# ============================================================
# 02 - Object-Oriented Programming (OOP)
# ============================================================
# Topics: class, __init__, methods, properties, inheritance,
#         dunder methods, class/static methods, dataclasses
# ============================================================

from dataclasses import dataclass, field
from abc import ABC, abstractmethod

# ------ Basic Class ------
class Engineer:
    # Class variable — shared by all instances
    company = "Tech Corp"
    headcount = 0

    def __init__(self, name: str, role: str, experience: int):
        # Instance variables — unique to each instance
        self.name = name
        self.role = role
        self.experience = experience
        Engineer.headcount += 1

    # Instance method
    def introduce(self) -> str:
        return f"Hi, I'm {self.name}, a {self.role} with {self.experience} years of exp."

    # String representation
    def __repr__(self) -> str:
        return f"Engineer(name={self.name!r}, role={self.role!r}, exp={self.experience})"

    def __str__(self) -> str:
        return f"{self.name} ({self.role})"

    # Comparison dunder methods
    def __eq__(self, other) -> bool:
        if not isinstance(other, Engineer):
            return NotImplemented
        return self.name == other.name and self.role == other.role

    def __lt__(self, other) -> bool:
        return self.experience < other.experience

    # Class method — operates on the class itself
    @classmethod
    def from_dict(cls, data: dict) -> "Engineer":
        return cls(data["name"], data["role"], data["experience"])

    @classmethod
    def get_headcount(cls) -> int:
        return cls.headcount

    # Static method — utility, doesn't need self or cls
    @staticmethod
    def is_senior(experience: int) -> bool:
        return experience >= 5


eng1 = Engineer("KruthikaDevi", "Data Engineer", 7)
eng2 = Engineer.from_dict({"name": "Alice", "role": "ML Engineer", "experience": 4})

print(eng1)
print(repr(eng1))
print(eng1.introduce())
print(Engineer.is_senior(7))    # True
print(Engineer.get_headcount()) # 2
print(sorted([eng1, eng2]))     # sorted by experience

# ------ Properties (Getter/Setter) ------
class DataPipeline:
    def __init__(self, name: str, batch_size: int):
        self.name = name
        self._batch_size = batch_size   # private by convention
        self.__secret = "hidden"        # name-mangled

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value: int):
        if value <= 0:
            raise ValueError("batch_size must be positive")
        self._batch_size = value

    @batch_size.deleter
    def batch_size(self):
        del self._batch_size

    @property
    def is_large_batch(self) -> bool:
        return self._batch_size > 1000

p = DataPipeline("ETL_Daily", 500)
print(p.batch_size)         # 500
p.batch_size = 1000
print(p.is_large_batch)     # True
# p.batch_size = -1         # ❌ ValueError

# ------ Inheritance ------
class DataWorker(ABC):   # Abstract Base Class
    def __init__(self, name: str, source: str):
        self.name = name
        self.source = source

    @abstractmethod
    def extract(self) -> list:
        """Must be implemented by subclasses."""
        pass

    @abstractmethod
    def transform(self, data: list) -> list:
        pass

    def run(self):
        data = self.extract()
        transformed = self.transform(data)
        print(f"[{self.name}] Processed {len(transformed)} records")
        return transformed


class SparkETL(DataWorker):
    def __init__(self, name, source, partition_count=200):
        super().__init__(name, source)
        self.partition_count = partition_count

    def extract(self) -> list:
        print(f"Reading from {self.source} with Spark")
        return [{"id": i, "value": i * 10} for i in range(100)]

    def transform(self, data: list) -> list:
        return [r for r in data if r["value"] > 200]


class GlueETL(DataWorker):
    def extract(self) -> list:
        print(f"Reading from {self.source} via AWS Glue")
        return [{"id": i, "raw": f"data_{i}"} for i in range(50)]

    def transform(self, data: list) -> list:
        return [{"id": r["id"], "clean": r["raw"].upper()} for r in data]


spark_job = SparkETL("Silver_Transform", "s3://raw-bucket/orders")
glue_job  = GlueETL("Glue_Ingest", "s3://landing-zone/events")

spark_job.run()
glue_job.run()

# isinstance / issubclass
print(isinstance(spark_job, DataWorker))   # True
print(isinstance(spark_job, SparkETL))     # True
print(issubclass(SparkETL, DataWorker))    # True

# ------ Multiple Inheritance & MRO ------
class Loggable:
    def log(self, msg): print(f"[LOG] {msg}")

class Schedulable:
    def schedule(self, cron): print(f"[SCHEDULE] {cron}")

class ManagedPipeline(SparkETL, Loggable, Schedulable):
    pass

mp = ManagedPipeline("Managed", "s3://data")
mp.log("Starting")
mp.schedule("0 2 * * *")
print(ManagedPipeline.__mro__)   # Method Resolution Order

# ------ Dataclasses ------
@dataclass
class ColumnSchema:
    name: str
    dtype: str
    nullable: bool = True
    default: str = None
    tags: list = field(default_factory=list)

    def spark_type(self) -> str:
        mapping = {"string": "StringType", "int": "IntegerType", "float": "DoubleType"}
        return mapping.get(self.dtype, "StringType")


@dataclass(order=True)
class TableSchema:
    table_name: str
    columns: list = field(default_factory=list, compare=False)

    def add_column(self, col: ColumnSchema):
        self.columns.append(col)

    def ddl(self) -> str:
        col_defs = ", ".join(f"{c.name} {c.dtype.upper()}" for c in self.columns)
        return f"CREATE TABLE {self.table_name} ({col_defs});"


schema = TableSchema("silver.orders")
schema.add_column(ColumnSchema("order_id", "int", nullable=False))
schema.add_column(ColumnSchema("customer_name", "string"))
schema.add_column(ColumnSchema("amount", "float"))
print(schema.ddl())
print(schema.columns[0].spark_type())

# ------ Practice Exercises ------
# 1. Create a BankAccount class with deposit, withdraw, and balance property.
# 2. Add __add__ to Engineer to combine experience of two engineers.
# 3. Create a Shape ABC with area() and perimeter(); implement Circle and Rectangle.
# 4. Use @dataclass to model a Spark job with name, schedule, cluster_size.
# 5. Demonstrate MRO conflict resolution using super() correctly.
