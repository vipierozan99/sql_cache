import uuid
from datetime import datetime, timezone

from sqlalchemy import ForeignKey
from sqlalchemy import types as SQLTypes
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    registry,
)


class UUID(SQLTypes.TypeDecorator):
    """Platform-independent GUID type.
    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.
    """

    impl = SQLTypes.UUID
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(SQLTypes.UUID())
        else:
            return dialect.type_descriptor(SQLTypes.CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == "postgresql":
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return str(uuid.UUID(value))
            else:
                return str(value)

    def _uuid_value(self, value):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value

    def process_result_value(self, value, dialect):
        return self._uuid_value(value)

    def sort_key_function(self, value):
        return self._uuid_value(value)

    @property
    def python_type(self):
        return uuid.UUID


class DATETIMETZ(SQLTypes.TypeDecorator):
    """
    Uses PostgreSQL's and SQLite datetime type
    but since sqlite does not store timezone info, assume UTC on conversion
    """

    impl = SQLTypes.DATETIME
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(SQLTypes.DateTime(timezone=True))

    def process_bind_param(self, value, dialect):
        # convert value to UTC on insert
        if dialect.name != "sqlite" or not isinstance(value, datetime):
            return value

        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)

        return value

    def process_result_value(self, value, dialect):
        # assume utc on retrieval
        if dialect.name != "sqlite" or not isinstance(value, datetime):
            return value

        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return value

    def sort_key_function(self, value):
        return self._tz_value(value)

    @property
    def python_type(self):
        return datetime


SQLRegistry = registry(
    type_annotation_map={
        datetime: DATETIMETZ(),
        uuid.UUID: UUID,
    }
)


class SQLBase(MappedAsDataclass, DeclarativeBase):
    registry = SQLRegistry


class MyUser(SQLBase):
    __tablename__ = "my_user"

    name: Mapped[str]
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default_factory=uuid.uuid4)


class MyRecord(SQLBase):
    __tablename__ = "my_record"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(
            f"{MyUser.__tablename__}.id",
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    data: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default_factory=uuid.uuid4)
