from sqlalchemy import Column, Integer, String, Float, Index
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import VARCHAR

Base = declarative_base()

class Tile(Base):
    __tablename__ = "tiles"

    id = Column(Integer, primary_key=True, index=True)
    # Для PostgreSQL можно использовать VARCHAR с ограничением длины
    filename = Column(VARCHAR(255), unique=True, index=True)
    
    top_left_lon = Column(Float)
    top_left_lat = Column(Float)
    bottom_right_lon = Column(Float)
    bottom_right_lat = Column(Float)
    
    mask_name = Column(VARCHAR(255))
    
    # Опционально: добавьте индексы для географических поисков
    __table_args__ = (
        Index('idx_coordinates', 'top_left_lat', 'top_left_lon', 
              'bottom_right_lat', 'bottom_right_lon'),
    )