from fastapi import APIRouter, HTTPException, Depends, Request, Query
from typing import Optional

# Router for trading view endpoints
router = APIRouter()

# Dependency to retrieve `symbol_finder` from app state
def get_symbol_finder(request: Request):
    symbol_finder = request.app.state.symbol_finder
    if not symbol_finder:
        raise HTTPException(status_code=500, detail="SymbolFinder is not initialized.")
    return symbol_finder



@router.get("/", 
         summary ='Extracts trading URLs for a specified base and quote currency pair.',
         tags=['extract tradingview url'])
def extract_tv_url(
    # cryptopair: CryptoPair=Depends(),
    BASE: str = Query('BTC', description="The base cryptocurrency, default is 'BTC'"),
    QUOTE: str = Query(None, description="The quote cryptocurrency, optional", example="USD"),
    symbol_finder = Depends(get_symbol_finder)):
    """
    Extracts trading view URLs based on base and optional quote currency.

    Parameters:
        BASE (str): The base currency symbol (e.g., BTC).
        QUOTE (Optional[str]): The quote currency symbol (e.g., USD). Defaults to None.

    Returns:
        list: A list of trading URLs for the specified base and quote currency.
    """
    try:
        return symbol_finder.extract_tv_url(BASE=BASE, QUOTE=QUOTE)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

