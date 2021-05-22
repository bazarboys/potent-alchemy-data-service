import logging
from collections.abc import Sequence
from sqlalchemy import and_, or_
from sqlalchemy.orm.decl_api import declarative_base, _declarative_constructor
import threading
from enum import Enum, unique
from sqlalchemy.inspection import inspect
from . import util


class CommonBase:
    @property
    @classmethod
    def column_names(cls):
        return NotImplementedError

    @property
    @classmethod
    def primary_keys(cls):
        return NotImplementedError

    @property
    @classmethod
    def relationships(cls):
        return NotImplementedError

    @classmethod
    def initialize(cls):
        cls.column_names = [
            c.name for c in cls.__table__.columns  # pylint: disable=no-member
        ]
        cls.primary_keys = [
            c.name
            for c in cls.__table__.columns  # pylint: disable=no-member
            if c.primary_key
        ]
        # https://github.com/sqlalchemy/sqlalchemy/blob/master/lib/sqlalchemy/orm/base.py#L167-L195  # noqa
        cls.relationships = {
            r.key: {"class": r.mapper.class_, "direction": r.direction.name}
            for r in inspect(cls).relationships
        }

    def __repr__(self):
        cls = self.__class__
        cols = [f"'{x}': '{getattr(self, x)}'" for x in cls.column_names]
        # note that this will fail if there is not an active session and lazy
        # loading is enabled. See:
        # https://docs.sqlalchemy.org/en/13/errors.html#error-bhk3
        rels = [f"'{x}': '{getattr(self, x)}'" for x in cls.relationships]
        return f"<{cls.__name__}(" + ", ".join(cols + rels) + ")>"

    def get_as_dict(self, recursive=True):
        """
        Returns a dictionary with the contents of this object.
        """
        d = {k: getattr(self, k) for k in self.column_names if hasattr(self, k)}

        if not recursive:
            return d

        # convert relationship objects to dict
        for k in self.relationships:
            if not hasattr(self, k):
                continue
            d[k] = [x.get_as_dict(False) for x in getattr(self, k)]

        return d

    def set_fields(self, **kwargs):
        d = kwargs.copy()

        # for relationships convert dictionaries to orm objects
        for k, meta in self.relationships.items():  # pylint: disable=no-member
            _class = meta["class"]
            if k not in d or not d[k] or isinstance(d[k][0], _class):
                continue
            d[k] = [_class(**x) for x in d[k]]

        _declarative_constructor(self, **d)

    def __init__(self, **kwargs):
        """
        This is only called when `declarative_base` is called with
        `constructor=None`. We override in order to convert relationship
        fields to their respective classes if they are defined as dictionaries.
        """
        self.set_fields(**kwargs)


Base = declarative_base(cls=CommonBase, constructor=None)


@unique
class RecordType(Enum):
    """
    Definitions for record types. Used for specifying return types on read()
    and query(). Currently supports `DICT` (dictionary) and `ORM` (orm object).
    """

    DICT = 1
    ORM = 2


class DatabaseHelper(object):
    def __init__(self, orm_class, session_factory, initialization_callback=None):
        self.orm_class = orm_class
        self.table_name = orm_class.__tablename__
        self.column_names = orm_class.column_names
        self.primary_keys_list = orm_class.primary_keys
        self.primary_keys = frozenset(orm_class.primary_keys)
        self.logger = logging.getLogger(orm_class.__name__)
        self.session_factory = session_factory
        self.initialization_callback = initialization_callback
        self.initialized = False
        self.initialized_lock = threading.RLock()

    def _initialize(self):
        """
        Initialize this object if not already.
        """
        with self.initialized_lock:
            if not self.initialized:
                if self.initialization_callback:
                    self.logger.info("Calling initialization callback")
                    self.initialization_callback()
                self.initialized = True

    def _get_primary_key_record(self, record):
        """
        Return a new record dictionary containing only the table primary keys.
        Throws `ValueError` if any keys are missing.
        """
        missing_keys = self.primary_keys - record.keys()
        if missing_keys:
            raise ValueError(f"Primary keys {missing_keys} not in record")
        return {k: record[k] for k in self.primary_keys}

    def add(self, record, session=None, check_exists=False):
        """
        Add a new record to the database.

        Arguments:
            record {dict}   :   The new record to add.
            session         :   A session object to reuse, creates and uses a
                                new session if not given.
        """
        self._initialize()

        primary_record = self._get_primary_key_record(record)
        auto_session = False
        action = "queued add()"

        try:
            if session is None:
                auto_session = True
                session = self.session_factory()
                action = "add()"

            exists = False
            if check_exists:
                columns = [getattr(self.orm_class, x) for x in primary_record]
                query = session.query(*columns).filter_by(**primary_record)
                exists = query.scalar() is not None

            if exists:
                raise ValueError(
                    f"{action} record: '{primary_record}' already exists in "
                    f"the table '{self.table_name}'."
                )

            else:
                orm_obj = self.orm_class(**record)
                session.add(orm_obj)

                if auto_session:
                    session.commit()

        except Exception as e:
            if auto_session:
                session.rollback()
            self.logger.error(
                f"{action} error with table '{self.table_name}': {str(e)}."
            )
            raise e

        finally:
            if auto_session:
                session.close()

        self.logger.debug(
            f"Successful {action} record {primary_record} with table "
            f"'{self.table_name}'."
        )

    def update(self, record, session=None):
        """
        Update a record in the database.

        Arguments:
            record {dict}   :   The record to update. Primary keys uniquely
                                identifying the record to update are required.
            session         :   A session object to reuse, creates and uses a
                                new session if not given.
        """
        self._initialize()

        primary_record = self._get_primary_key_record(record)
        auto_session = False
        action = "queued update()"

        try:
            if session is None:
                auto_session = True
                session = self.session_factory()
                action = "update()"

            query = session.query(self.orm_class).filter_by(**primary_record)
            result = query.first()

            if result:
                update_record = record.copy()
                for k in self.primary_keys:
                    update_record.pop(k, None)
                result.set_fields(**update_record)

                if auto_session:
                    session.commit()

            else:
                raise ValueError(
                    f"{action} record: {primary_record} not found in the "
                    f"table '{self.table_name}'."
                )

        except Exception as e:
            if auto_session:
                session.rollback()
            self.logger.error(
                f"{action} record: {primary_record} error with table "
                f"'{self.table_name}': {str(e)}."
            )
            raise e

        finally:
            if auto_session:
                session.close()

        self.logger.debug(
            f"Successful {action} record {primary_record} with table "
            f"'{self.table_name}'."
        )

    def delete(self, record, session=None):
        """
        Delete a record from the database.

        Arguments:
            record {dict}   :   The record to delete. Primary keys identifying
                                the record to delete are required.
            session         :   A session object to reuse, creates and uses a
                                new session if not given.
        """
        self._initialize()

        primary_record = self._get_primary_key_record(record)
        auto_session = False
        action = "queued delete()"

        try:
            if session is None:
                auto_session = True
                session = self.session_factory()
                action = "delete()"

            query = session.query(self.orm_class).filter_by(**primary_record)
            result = query.first()

            if result:
                session.delete(result)
                if auto_session:
                    session.commit()
            else:
                raise ValueError(
                    f"{action} record: {primary_record} not found in the "
                    f"table '{self.table_name}'."
                )

        except Exception as e:
            if auto_session:
                session.rollback()
            self.logger.error(
                f"{action} record: {primary_record} error with table "
                f"'{self.table_name}': {str(e)}."
            )
            raise e

        finally:
            if auto_session:
                session.close()

        self.logger.debug(
            f"Successful {action} record {primary_record} with table "
            f"'{self.table_name}'."
        )

    def _get_result_as(self, result, record_type):
        if record_type == RecordType.DICT:
            if hasattr(result, "get_as_dict"):
                return result.get_as_dict()
            else:
                return {
                    k: getattr(result, k)
                    for k in self.column_names
                    if hasattr(result, k)
                }
        elif record_type == RecordType.ORM:
            return result
        else:
            raise ValueError(f"Invalid record_type: {record_type}")

    def read(
        self,
        filter_by,
        session=None,
        order_by=None,
        limit=None,
        record_type=RecordType.DICT,
    ):
        """
        Retrieves records from the database based on values set in the input
        dictionary.

        Arguments:
            filter_by   :   Dictionary of input, filters are applied based on
                            what is passed in the dictionary (equals operator)
            session     :   A session object to reuse, creates and uses a new
                            session if not given.
            order_by    :   An `order_by` definition in ORM notation.
            limit       :   The number of rows to return, default all.
            record_type :   The type of record list to to return. See
                            RecordType.

        Returns:
            records     :   List of all the records from the database that was
                            obtained applying the `filter_by`.
        """
        self._initialize()

        fetched_records = []
        auto_session = False
        action = "queued read()"

        try:
            if session is None:
                auto_session = True
                session = self.session_factory()
                action = "read()"

            query = session.query(self.orm_class).filter_by(**filter_by)
            if order_by is not None:  # cannot be 'if not order_by'
                query = query.order_by(order_by)
            if limit is not None:
                query = query.limit(limit)
            result = query.all()

            if result:
                for r in result:
                    fetched_records.append(self._get_result_as(r, record_type))

        except Exception as e:
            if auto_session:
                session.rollback()
            self.logger.error(
                f"{action} error with table '{self.table_name}': {str(e)}."
            )
            raise e

        finally:
            if auto_session:
                session.close()

        self.logger.debug(
            f"Successful {action} {len(fetched_records)} records from the "
            f"table '{self.table_name}'."
        )

        return fetched_records

    def query(
        self,
        criterion,
        entities=None,
        session=None,
        order_by=None,
        limit=None,
        record_type=RecordType.DICT,
    ):
        """
        Retrieves records from the database based on the criterion specified.
        See query.filter() for more details:
        https://docs.sqlalchemy.org/en/13/orm/tutorial.html#querying

        Arguments:
            criterion   :   Filters applied to the query using ORM notation.
            entities    :   The columns to `select` using ORM notation.
            session     :   A session object to reuse, creates and uses a new
                            session if not given.
            order_by    :   An `order_by` definition in ORM notation.
            limit       :   The number of rows to return, default all.
            record_type :   The type of record list to to return. See
                            RecordType.

        Returns:
            records     :   List of all the records from the database that was
                            obtained running the query.
        """
        self._initialize()

        # if entities are not specified get all columns
        if not entities:
            entities = [self.orm_class]
        elif not isinstance(entities, Sequence) or isinstance(entities, str):
            entities = [entities]

        fetched_records = []
        auto_session = False
        action = "queued query()"

        try:
            if session is None:
                auto_session = True
                session = self.session_factory()
                action = "query()"

            query = session.query(*entities).filter(criterion)
            if order_by is not None:  # cannot be 'if not order_by'
                query = query.order_by(order_by)
            if limit is not None:
                query = query.limit(limit)
            result = query.all()

            if result:
                for r in result:
                    fetched_records.append(self._get_result_as(r, record_type))

        except Exception as e:
            if auto_session:
                session.rollback()
            self.logger.error(
                f"{action} error with table '{self.table_name}': {str(e)}."
            )
            raise e

        finally:
            if auto_session:
                session.close()

        self.logger.debug(
            f"Successful {action} {len(fetched_records)} records from the "
            f"table '{self.table_name}'."
        )

        return fetched_records

    def primary_key_dict_to_tuple(self, pk_dict):
        """
        Transform a dictionary containing primary key values to a tuple
        using the `primary_keys` ordering of the ORM object. For example:
        `{'id': 'id1', 'col', 'col1'}` becomes `('id1', 'col1')`. Where
        `primary_keys` is set to `['id', 'col']`. Useful for working with the
        `exists()` method.

        Arguments:
            pk_dict     : A dictionary of primary key values.

        Returns:
            tuple       : A tuple containing primary key values.
        """
        return util.dict_to_tuple(pk_dict, self.primary_keys_list)

    def primary_key_tuple_to_dict(self, pk_tuple):
        """
        Transform a tuple containing primary key values to a dictionary
        using the `primary_keys` ordering of the ORM object. For example:
        `('id1', 'col1')` becomes `{'id': 'id1', 'col', 'col1'}`. Where
        `primary_keys` is set to `['id', 'col']`. Useful for working with the
        `exists()` method.

        Arguments:
            pk_tuple    : A tuple containing primary key values.

        Returns:
            dict        : A dictionary of primary key values.
        """
        return util.tuple_to_dict(pk_tuple, self.primary_keys_list)

    def exists(self, records, session=None):
        """
        Performs a query to check that the records given are in the DB or not.
        The primary keys are used for the query.

        Examples:

            # single value
            helper.exists("id1")
            helper.exists({"id1": "id1"})
            helper.exists(("id1",))

            # multiple values
            helper.exists(["id1", "id2"])
            helper.exists([{"id1": "id1"}, {"id1": "id2"}])
            helper.exists([("id1",), ("id2",)])

            # multiple primary key columns requires tuples or dicts
            # single value
            helper.exists(("id1", "tag1",))
            helper.exists({"id": "id1", "tag": "tag1"})
            # multiple values
            helper.exists([("id1", "tag1",),
                           ("id2", "tag2",)])
            helper.exists([{"id": "id1", "tag": "tag1"},
                           {"id": "id2", "tag": "tag2"}])

        Arguments:
            records     :   Primary key values to test. Input can be given as
                            single values or lists of values. Values can be in
                            dictionary or tuple format. For tables with single
                            primary keys columns, single values are supported.
            session     :   A session object to reuse; creates and uses a new
                            session if not given.

        Returns:
            exists      :   For single value inputs, True or False.
                            For list inputs, a list of boolean values
                            corresponding to the input list.
        """
        self._initialize()

        if records is None:
            return None

        # convert input into a list of tuples; support single value input
        single_value = False
        invalid_types = (bytes, bytearray)
        if isinstance(records, invalid_types):
            raise ValueError(f"Input type '{type(records)}' not supported")
        elif isinstance(records, tuple):
            single_value = True
            records = [records]
        elif isinstance(records, dict):
            single_value = True
            records = [self.primary_key_dict_to_tuple(records)]
        elif not isinstance(records, list):
            if len(self.primary_keys_list) != 1:
                raise ValueError(
                    f"Input type '{type(records)}' invalid for tables with "
                    "multiple primary keys"
                )
            single_value = True
            records = [tuple([records])]
        else:  # a list
            if all(isinstance(x, tuple) for x in records):
                pass
            elif all(isinstance(x, dict) for x in records):
                records = [self.primary_key_dict_to_tuple(x) for x in records]
            elif any(isinstance(x, invalid_types) for x in records):
                raise ValueError(
                    f"Lists of one of {invalid_types} are not " "supported"
                )
            else:
                # list of objects -- convert to a list of tuples
                if len(self.primary_keys_list) != 1:
                    raise ValueError(
                        "Lists of non-tuple and non-dict are invalid for "
                        "tables with multiple primary keys"
                    )
                records = [tuple([x]) for x in records]

        if not records:
            if single_value:
                return None
            else:
                return {}

        columns = [getattr(self.orm_class, x) for x in self.primary_keys_list]

        # use the IN statement in the query if there is a single primary key,
        # otherwise we need to construct a complex WHERE clause

        if len(self.primary_keys_list) == 1:
            keys = [x[0] for x in records]
            criterion = columns[0].in_(keys)
        else:
            or_clauses = []
            for record in records:
                and_clauses = []
                for value, column in zip(record, columns):
                    and_clauses.append(column == value)
                or_clauses.append(and_(*and_clauses))
            criterion = or_(*or_clauses)

        results = self.query(criterion, entities=columns, session=session)
        existing_rows = {self.primary_key_dict_to_tuple(result) for result in results}
        exists_list = [record in existing_rows for record in records]

        if single_value:
            return exists_list[0]
        else:
            return exists_list
