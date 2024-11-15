# app/module_loader.py

from services.symbol_finder import SymbolFinder
# Additional dependencies can be imported and initialized here as needed

def load_dependencies():
    """
    Initializes and returns the application's dependencies.
    """
    # Initialize SymbolFinder or any other dependencies
    symbol_finder = SymbolFinder()
    
    # You can add more dependencies here if necessary
    return {
        "symbol_finder": symbol_finder
    }
