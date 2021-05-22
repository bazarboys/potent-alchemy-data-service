from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import threading


class SessionFactory(object):
    """
    Factory class for generating DB session objects. Created with:
    ```python
    session_factory = SessionFactory('some:db:connection:string')
    ```
    Can also be constructed with a callable that returns a `connection_string`.
    """

    def __init__(self, connection_string):
        self.sessionmaker = None
        self.sessionmaker_lock = threading.RLock()
        self.connection_string = connection_string

    def __call__(self):
        with self.sessionmaker_lock:
            if not self.sessionmaker:
                connection_string = self.connection_string
                if callable(connection_string):
                    connection_string = connection_string()
                engine = create_engine(connection_string)
                self.sessionmaker = sessionmaker(bind=engine)
        return self.sessionmaker()


@contextmanager
def session_scope(session_factory):
    """
    Context helper to wrap session code in a `with` block. Takes care of
    `commit()`, `rollback()` and `close()`. For example:
    ```python
    with session_scope(session_factory) as session:
        do_db_stuff_with_session(session)
    ```
    """
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
