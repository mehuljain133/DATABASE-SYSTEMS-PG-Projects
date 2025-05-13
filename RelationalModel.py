# Unit-III Relational Model & Relational Data Manipulations: Relation, conversion of ER diagrams to relations, integrity constraints, relational algebra, relational domain & tuple calculus. 

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.exc import IntegrityError

Base = declarative_base()
engine = create_engine("sqlite:///relational_model.db", echo=False)
Session = sessionmaker(bind=engine)
session = Session()

# ----------------------------------------
# Relational Model (ER to Relations)
# ----------------------------------------

class Department(Base):
    __tablename__ = 'departments'
    dept_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    
    students = relationship("Student", back_populates="department")

class Student(Base):
    __tablename__ = 'students'
    roll_no = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    dept_id = Column(Integer, ForeignKey('departments.dept_id'))

    department = relationship("Department", back_populates="students")

    # INTEGRITY CONSTRAINT: Unique roll number
    __table_args__ = (UniqueConstraint('roll_no', name='uq_roll_no'),)

# -----------------------------
# Initialize DB and Insert Data
# -----------------------------
def initialize_database():
    Base.metadata.create_all(engine)

    if not session.query(Department).first():
        # Insert sample departments and students
        d1 = Department(name='Computer Science')
        d2 = Department(name='Physics')
        session.add_all([d1, d2])

        s1 = Student(roll_no=101, name="Alice", department=d1)
        s2 = Student(roll_no=102, name="Bob", department=d1)
        s3 = Student(roll_no=201, name="Charlie", department=d2)

        session.add_all([s1, s2, s3])
        try:
            session.commit()
        except IntegrityError:
            session.rollback()

# ------------------------------------------
# Relational Algebra (Simulated with SQL)
# ------------------------------------------

def relational_algebra_simulation():
    print("\n🔍 Projection: Names of Students")
    result = engine.execute("SELECT name FROM students")
    for row in result:
        print(row[0])

    print("\n🔍 Selection: Students in Computer Science")
    result = engine.execute("""
        SELECT s.name FROM students s
        JOIN departments d ON s.dept_id = d.dept_id
        WHERE d.name = 'Computer Science'
    """)
    for row in result:
        print(row[0])

    print("\n🔍 Join: Student Name with Department Name")
    result = engine.execute("""
        SELECT s.name, d.name FROM students s
        JOIN departments d ON s.dept_id = d.dept_id
    """)
    for row in result:
        print(f"{row[0]} - {row[1]}")

# -------------------------------------------------
# Domain & Tuple Calculus (SQL equivalents)
# -------------------------------------------------

def relational_calculus_simulation():
    print("\n📐 Domain Calculus: List all (name, dept) pairs")
    result = engine.execute("""
        SELECT s.name, d.name FROM students s, departments d
        WHERE s.dept_id = d.dept_id
    """)
    for row in result:
        print(f"{row[0]} ({row[1]})")

    print("\n📐 Tuple Calculus: Find students where EXISTS department")
    result = engine.execute("""
        SELECT s.name FROM students s
        WHERE EXISTS (
            SELECT 1 FROM departments d WHERE d.dept_id = s.dept_id
        )
    """)
    for row in result:
        print(row[0])

# -----------------------------
# Run All Sections
# -----------------------------
if __name__ == "__main__":
    # Initialize database and insert sample data
    initialize_database()

    # Relational Algebra Simulation
    relational_algebra_simulation()

    # Relational Calculus Simulation
    relational_calculus_simulation()

