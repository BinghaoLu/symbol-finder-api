from fastapi import FastAPI
from module_loader import load_dependencies
from routers import trading_view, crypto_exchange
from contextlib import asynccontextmanager

# Initialize the FastAPI app with a lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load dependencies during startup
    try:
        dependencies = load_dependencies()
        app.state.symbol_finder = dependencies["symbol_finder"]  # Store in app state
        print("Dependencies loaded successfully.")
        yield  # Wait for the app to run
    except Exception as e:
        print(f"Error during startup: {str(e)}")
        raise e
    finally:
        # Cleanup logic during shutdown
        print("Cleaning up resources...")
        # Add any cleanup logic here if necessary (e.g., closing files, connections)
        app.state.symbol_finder = None

app = FastAPI(title="Modular FastAPI App", lifespan=lifespan)

# Include routers
app.include_router(trading_view.router, prefix="/trading_view", tags=["Trading View"])
app.include_router(crypto_exchange.router, prefix="/crypto_exchange", tags=["Crypto Exchange"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
