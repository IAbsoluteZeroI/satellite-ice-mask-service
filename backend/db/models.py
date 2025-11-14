from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Tile(Base):
    __tablename__ = "tiles"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, unique=True, index=True)

    top_left_lon = Column(Float)
    top_left_lat = Column(Float)
    bottom_right_lon = Column(Float)
    bottom_right_lat = Column(Float)
    
    mask_name = Column(String)
