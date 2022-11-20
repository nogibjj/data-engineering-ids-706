from fastapi import FastAPI
import yfinance

app = FastAPI()


@app.get("/healthCheck")
async def root():
    return {"message": "OK"}

@app.get("/get_current_price/{ticker}")
async def get_stock(ticker: str):
    try:
        stock = yfinance.Ticker(ticker)
        return {"ticker": ticker, "price": stock.info["regularMarketPrice"]}
    except Exception as ee:
        return {"error": str(ee)}

@app.get("/get_historical_price/{ticker}/{start_date}/{end_date}")
async def get_historical_stock(ticker: str, start_date: str, end_date: str):
    try:
        stock = yfinance.Ticker(ticker)
        return {"ticker": ticker, "price": stock.history(start=start_date, end=end_date)["Close"].tolist()}
    except Exception as ee:
        return {"error": str(ee)}

@app.get("/financials/{ticker}")
async def get_financials(ticker: str):
    try:
        stock = yfinance.Ticker(ticker)
        return {"ticker": ticker, "financials": stock.financials.to_dict()}
    except Exception as ee:
        return {"error": str(ee)}

@app.get("analyst_recommendation/{ticker}")
async def get_recommendation(ticker: str):
    
    try:
        stock = yfinance.Ticker(ticker)
        return {"ticker": ticker, "recommendation": stock.recommendations.to_dict()}
    except Exception as ee:
        return {"error": str(ee)}

@app.get("/get_dividends/{ticker}")
async def get_dividends(ticker: str):
    try:
        stock = yfinance.Ticker(ticker)
        return {"ticker": ticker, "dividends": stock.dividends.to_dict()}
    except Exception as ee:
        return {"error": str(ee)}

