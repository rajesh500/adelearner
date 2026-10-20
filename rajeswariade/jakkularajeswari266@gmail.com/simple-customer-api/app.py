import os
import uvicorn
from fastapi import FastAPI, HTTPException

app = FastAPI()

# Sample data stored in memory.
customers = {
    101: {
        "customer_id": 101,
        "name": "Rajesh",
        "city": "Chicago"
    },
    102: {
        "customer_id": 102,
        "name": "Priya",
        "city": "Dallas"
    }
}


@app.get("/")
def home():
    return {"message": "My Databricks API is running!"}


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: int):
    customer = customers.get(customer_id)

    if customer is None:
        raise HTTPException(
            status_code=404,
            detail="Customer not found"
        )

    return customer


if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port
    )
