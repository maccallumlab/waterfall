from sqlalchemy import Column, ForeignKey, Integer, Float, PickleType, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref, scoped_session
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine


# Check to make that parent_id exists when a new
# structure is added.
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


Base = declarative_base()


class Structure(Base):
    __tablename__ = "structures"
    id = Column(Integer, primary_key=True)
    stage = Column(Integer)
    work = Column(Float)
    copies = Column(Integer)
    multiplicity = Column(Integer)
    parent_id = Column(Integer, ForeignKey("structures.id"))
    structure = Column(PickleType)
    children = relationship("Structure")
    parent = relationship("Structure", remote_side=[id])


class Vault:
    def __init__(self, DBSession, n_stages):
        self.DBSession = DBSession
        self.n_stages = n_stages

    def save_structure(self, stage, work, copies, multiplicity, parent_id, structure):
        session = self.DBSession()
        entry = Structure(
            stage=stage,
            work=work,
            copies=copies,
            multiplicity=multiplicity,
            parent_id=parent_id,
            structure=structure,
        )
        session.add(entry)
        session.commit()
        return entry.id

    def get_traj_starts(self):
        session = self.DBSession()
        return session.query(Structure).filter(Structure.stage == -1).all()

    def get_traj_ends(self):
        session = self.DBSession()
        return (
            session.query(Structure)
            .filter(Structure.stage == (self.n_stages - 1))
            .all()
        )

    def get_all(self):
        session = self.DBSession()
        return session.query(Structure).all()

    def update_copies(self, id, copies):
        session = self.DBSession()
        s = session.query(Structure).filter(Structure.id == id).first()
        s.copies = copies
        session.commit()


def create_db(w):
    engine = create_engine("sqlite:///Data/waterfall.sqlite")
    Base.metadata.create_all(engine)
    Base.metadata.bind = engine
    DBSession = scoped_session(sessionmaker(bind=engine))
    return Vault(DBSession, w.n_stages)


def connect_db(w):
    engine = create_engine("sqlite:///Data/waterfall.sqlite")
    Base.metadata.bind = engine
    DBSession = scoped_session(sessionmaker(bind=engine))
    return Vault(DBSession, w.n_stages)


def connect_db_readonly(w):
    engine = create_engine("sqlite:///Data/waterfall.sqlite")
    Base.metadata.bind = engine
    DBSession = scoped_session(sessionmaker(bind=engine))
    return Vault(DBSession, w.n_stages)
