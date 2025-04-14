from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
import logging
import time
from typing import TypeVar, Type, List, Optional, Dict, Any, Union
from contextlib import contextmanager

# Set up structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type variable for models
T = TypeVar("T")


class DBContext:
    """Database context manager for handling SQLAlchemy sessions.

    This class provides a context manager for database operations using SQLAlchemy,
    with connection pooling, transaction management, and efficient query execution.

    Attributes:
        connection_url: The SQLAlchemy connection URL.
        engine: The SQLAlchemy engine instance.
        Session: The SQLAlchemy session factory.
        _session: The current session being used within the context.
    """

    # Class-level connection pool for reuse across instances
    _engines = {}

    def __init__(
        self,
        connection_url: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
    ):
        """Initialize the database context.

        Args:
            connection_url: The SQLAlchemy connection URL.
            pool_size: The size of the connection pool.
            max_overflow: The maximum overflow size of the pool.
            pool_timeout: The number of seconds to wait before giving up on getting a connection.
            pool_recycle: Number of seconds after which a connection is recycled.
        """
        self.connection_url = connection_url
        self._session = None

        # Create or get existing engine with connection pooling
        if connection_url not in self._engines:
            self._engines[connection_url] = create_engine(
                connection_url,
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                echo=False,  # Set to True for SQL query logging
            )

            # Add engine event listeners for monitoring
            @event.listens_for(self._engines[connection_url], "checkout")
            def receive_checkout(dbapi_conn, conn_record, conn_proxy):
                conn_record.info["checkout_time"] = time.time()

            @event.listens_for(self._engines[connection_url], "checkin")
            def receive_checkin(dbapi_conn, conn_record):
                checkout_time = conn_record.info.get("checkout_time")
                if checkout_time is not None:
                    total_time = time.time() - checkout_time
                    if total_time > 5:  # Log slow connections
                        logger.warning(f"Connection held for {total_time:.2f} seconds")

        self.engine = self._engines[connection_url]

        # Create thread-local session factory
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    def __enter__(self):
        """Enter the context manager, creating a new session.

        Returns:
            self: The database context instance.
        """
        self._session = self.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager, closing the session.

        Args:
            exc_type: The exception type, if any.
            exc_val: The exception value, if any.
            exc_tb: The exception traceback, if any.
        """
        if self._session:
            if exc_type:
                self._session.rollback()
                logger.error(f"Transaction rolled back due to: {exc_val}")
            self._session.close()
            self.Session.remove()  # Remove thread-local session

    @contextmanager
    def transaction(self):
        """Context manager for explicit transaction handling.

        Yields:
            The current session.
        """
        try:
            yield self._session
            self._session.commit()
        except Exception as e:
            self._session.rollback()
            logger.error(f"Transaction error: {str(e)}")
            raise

    def add(self, obj: T) -> T:
        """Add an object to the session and commit.

        Args:
            obj: The object to add.

        Returns:
            The added object.
        """
        try:
            self._session.add(obj)
            self._session.commit()
            self._session.refresh(obj)
            return obj
        except SQLAlchemyError as e:
            self._session.rollback()
            logger.error(f"Error adding object: {str(e)}")
            raise

    def add_all(self, objects: List[T]) -> List[T]:
        """Add multiple objects in a single transaction.

        Args:
            objects: List of objects to add.

        Returns:
            The list of added objects.
        """
        try:
            self._session.add_all(objects)
            self._session.commit()
            for obj in objects:
                self._session.refresh(obj)
            return objects
        except SQLAlchemyError as e:
            self._session.rollback()
            logger.error(f"Error adding objects: {str(e)}")
            raise

    def get(self, model: Type[T], obj_id: int) -> Optional[T]:
        """Get an object by ID.

        Args:
            model: The model class.
            obj_id: The object ID.

        Returns:
            The object if found, None otherwise.
        """
        return self._session.get(model, obj_id)

    def get_by_filter(
        self, model: Type[T], return_all: bool = False, **kwargs
    ) -> Union[Optional[T], List[T]]:
        """Get objects by filter criteria.

        Args:
            model: The model class.
            return_all: Whether to return all matching objects.
            **kwargs: Filter criteria.

        Returns:
            A single object, a list of objects, or None.
        """
        query = self._session.query(model).filter_by(**kwargs)

        if return_all:
            return query.all()
        else:
            return query.first()

    def get_by_ids(self, model: Type[T], ids: List[int]) -> List[T]:
        """Get multiple objects by their IDs efficiently.

        Args:
            model: The model class.
            ids: List of object IDs.

        Returns:
            List of objects.
        """
        if not ids:
            return []

        return self._session.query(model).filter(model.id.in_(ids)).all()

    def update(self, model: Type[T], obj_id: int, **kwargs) -> Optional[T]:
        """Update an object by ID.

        Args:
            model: The model class.
            obj_id: The object ID.
            **kwargs: Attributes to update.

        Returns:
            The updated object if found, None otherwise.
        """
        obj = self.get(model, obj_id)
        if not obj:
            return None

        try:
            for key, value in kwargs.items():
                setattr(obj, key, value)
            self._session.commit()
            return obj
        except SQLAlchemyError as e:
            self._session.rollback()
            logger.error(f"Error updating object ID {obj_id}: {str(e)}")
            raise

    def bulk_update(
        self, model: Type[T], id_attribute_map: Dict[int, Dict[str, Any]]
    ) -> int:
        """Update multiple objects in bulk.

        Args:
            model: The model class.
            id_attribute_map: Dictionary mapping object IDs to attribute dictionaries.

        Returns:
            Number of updated objects.
        """
        if not id_attribute_map:
            return 0

        try:
            count = 0
            for obj_id, attributes in id_attribute_map.items():
                obj = self.get(model, obj_id)
                if obj:
                    for key, value in attributes.items():
                        setattr(obj, key, value)
                    count += 1

            self._session.commit()
            return count
        except SQLAlchemyError as e:
            self._session.rollback()
            logger.error(f"Error in bulk update: {str(e)}")
            raise

    def delete(self, model: Type[T], obj_id: int) -> bool:
        """Delete an object by ID.

        Args:
            model: The model class.
            obj_id: The object ID.

        Returns:
            True if deleted, False if not found.
        """
        obj = self.get(model, obj_id)
        if not obj:
            return False

        try:
            self._session.delete(obj)
            self._session.commit()
            return True
        except SQLAlchemyError as e:
            self._session.rollback()
            logger.error(f"Error deleting object ID {obj_id}: {str(e)}")
            raise

    def bulk_delete(self, model: Type[T], ids: List[int]) -> int:
        """Delete multiple objects by IDs.

        Args:
            model: The model class.
            ids: List of object IDs.

        Returns:
            Number of deleted objects.
        """
        if not ids:
            return 0

        try:
            result = (
                self._session.query(model)
                .filter(model.id.in_(ids))
                .delete(synchronize_session=False)
            )
            self._session.commit()
            return result
        except SQLAlchemyError as e:
            self._session.rollback()
            logger.error(f"Error in bulk delete: {str(e)}")
            raise

    def execute_raw_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a raw SQL query.

        Args:
            query: The SQL query string.
            params: Query parameters.

        Returns:
            List of dictionaries with query results.
        """
        try:
            result = self._session.execute(query, params or {})
            return [dict(row) for row in result]
        except SQLAlchemyError as e:
            logger.error(f"Error executing raw query: {str(e)}")
            raise
