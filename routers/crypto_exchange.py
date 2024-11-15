from fastapi import APIRouter, HTTPException,Request, Depends
from services.symbol_finder import SymbolFinder
from fastapi.responses import FileResponse


# Dependency to retrieve `symbol_finder` from app state
def get_symbol_finder(request: Request):
    symbol_finder = request.app.state.symbol_finder
    if not symbol_finder:
        raise HTTPException(status_code=500, detail="SymbolFinder is not initialized.")
    return symbol_finder

router = APIRouter()


@router.get("/extract_crypto_exchange/",
         summary='Extracts exchange data from a unique symbol CSV file, processes it, and saves the result as a Parquet file.',
         tags=['extract_crypto_exchange'])
def extract_crypto_exchange(
    symbol_finder = Depends(get_symbol_finder)
):
    """
    This function reads data from a unique symbol CSV file, processes it to list different base-quote pairs available on various exchanges, and saves the processed data as a Parquet file.

    Returns:
        FileResponse: A downloadable Parquet file (`output.parquet`) containing the processed exchange data.

    """

    try:
        # file_path = symbol_finder.extract_crypto_exchange()
        return FileResponse(path='unique_symbols.csv', filename="output.parquet", media_type="application/octet-stream")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
