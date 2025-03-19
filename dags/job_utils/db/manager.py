from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Dict
from threading import Lock

class DBManager:
    _instances: Dict[str, "DBManager"] = {}
    _lock = Lock()

    def __new__(cls, user_id: str, connection_url: str):
        with cls._lock:
            if user_id not in cls._instances:
                instance = super().__new__(cls)
                engine = create_engine(connection_url)
                Session = sessionmaker(bind=engine)
                instance._session = Session()
                cls._instances[user_id] = instance
            return cls._instances[user_id]

    def add(self, obj):
        self._session.add(obj)
        self._session.commit()
        return obj

    def get(self, model, obj_id: int):
        return self._session.query(model).get(obj_id)

    def get_by_filter(self, model, return_all: bool = False, **kwargs):
        if return_all:
            return self._session.query(model).filter_by(**kwargs).all()
        else:
            return self._session.query(model).filter_by(**kwargs).first()

    def update(self, obj_id: int, model, **kwargs):
        obj = self.get(model, obj_id)
        for key, value in kwargs.items():
            setattr(obj, key, value)
        self._session.commit()
        return obj

    def delete(self, obj_id: int, model):
        obj = self.get(model, obj_id)
        self._session.delete(obj)
        self._session.commit()

    def close(self):
        self._session.close()

class DBContext:
    def __init__(self, connection_url):
        self.connection_url = connection_url
        self.session = None
        
    def __enter__(self):
        engine = create_engine(self.connection_url)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
            
    # Database operation methods
    def add(self, obj):
        self.session.add(obj)
        self.session.commit()
        return obj
    
    def get(self, model, obj_id: int):
        return self._session.query(model).get(obj_id)

    def get_by_filter(self, model, return_all: bool = False, **kwargs):
        if return_all:
            return self._session.query(model).filter_by(**kwargs).all()
        else:
            return self._session.query(model).filter_by(**kwargs).first()

    def update(self, obj_id: int, model, **kwargs):
        obj = self.get(model, obj_id)
        for key, value in kwargs.items():
            setattr(obj, key, value)
        self._session.commit()
        return obj

    def delete(self, obj_id: int, model):
        obj = self.get(model, obj_id)
        self._session.delete(obj)
        self._session.commit()