# Unit-VI Transaction Management: ACID properties, Concurrency Control in databases, transaction recovery. 

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, exc
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import IntegrityError
import random
import time

# Setting up the base and engine
Base = declarative_base()
engine = create_engine("sqlite:///transaction_management.db", echo=True)
Session = sessionmaker(bind=engine)
session = Session()

# -----------------------------------------
# Transaction Simulation (ACID, Recovery)
# -----------------------------------------

class Account(Base):
    __tablename__ = 'accounts'
    account_id = Column(Integer, primary_key=True)
    account_name = Column(String, nullable=False)
    balance = Column(Integer, nullable=False)

# ------------------------
# ACID Transaction Model
# ------------------------

def initialize_database():
    Base.metadata.create_all(engine)

    if not session.query(Account).first():
        # Insert initial account balances
        a1 = Account(account_name='Alice', balance=500)
        a2 = Account(account_name='Bob', balance=300)
        session.add_all([a1, a2])
        session.commit()

# ------------------------
# ACID Transaction Simulation
# ------------------------

def transfer_funds(sender_id, receiver_id, amount):
    try:
        # Begin a transaction (ACID starts)
        sender = session.query(Account).filter_by(account_id=sender_id).first()
        receiver = session.query(Account).filter_by(account_id=receiver_id).first()

        if sender.balance >= amount:
            # Deduct from sender's account
            sender.balance -= amount
            # Add to receiver's account
            receiver.balance += amount
            session.commit()  # ACID Commit: Make transaction permanent
            print(f"Transaction successful! {amount} transferred from {sender.account_name} to {receiver.account_name}")
        else:
            raise Exception("Insufficient funds")

    except Exception as e:
        # Rollback transaction on failure (ACID rollback)
        session.rollback()
        print(f"Transaction failed: {e}")

# ------------------------
# Concurrency Control (Locking)
# ------------------------

def concurrent_transaction_simulation():
    def simulate_concurrent_transactions():
        sender_id = 1  # Alice
        receiver_id = 2  # Bob
        amount = random.randint(1, 100)
        
        print(f"Attempting to transfer {amount} between Alice and Bob...")

        try:
            transfer_funds(sender_id, receiver_id, amount)
        except:
            print(f"Error occurred during transaction.")

    # Simulate concurrency by running multiple transactions
    for _ in range(5):
        simulate_concurrent_transactions()
        time.sleep(0.5)  # Delay to simulate concurrent transactions

# ------------------------
# Transaction Recovery (Logging, Rollback)
# ------------------------

def simulate_transaction_recovery():
    # Simulate a failed transaction that needs recovery
    print("\nSimulating a failed transaction with recovery...")

    try:
        transfer_funds(1, 2, 600)  # Alice tries to send more money than she has
    except Exception as e:
        print(f"Transaction error: {e}")

    # Rollback and check if database state is consistent (recovery)
    print("Attempting recovery (rollback)...")
    session.rollback()

    # Verify account balances after rollback
    alice = session.query(Account).filter_by(account_name='Alice').first()
    bob = session.query(Account).filter_by(account_name='Bob').first()

    print(f"Alice's balance after rollback: {alice.balance}")
    print(f"Bob's balance after rollback: {bob.balance}")

# -----------------------------
# Running the Code
# -----------------------------

if __name__ == "__main__":
    # Initialize the database and insert initial data
    initialize_database()

    # Simulate multiple transactions (ACID)
    transfer_funds(1, 2, 100)  # Alice transfers 100 to Bob

    # Test concurrency control (Simulate simultaneous transactions)
    concurrent_transaction_simulation()

    # Simulate transaction recovery (Rollback and check consistency)
    simulate_transaction_recovery()

