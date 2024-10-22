import asyncio
import aiohttp
import logging
import json
from datetime import datetime
from typing import List, Dict


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Set up defaults 
POOL_SIZE = 10
RETRY_COUNT = 3
FAILURE_THRESHOLD = 5


# Utility functions
async def get_session():
    async with aiohttp.ClientSession() as session:
        return session


async def do_api_call(item, session):
	try:
		await asyncio.sleep(0.1)  # Simulate API call
		return {
			'id': item['id'],
			'status': 'success',
			'timestamp': str(datetime.now()),
			'data': item['data']
		}
	except aiohttp.ClientError as e:
		logger.error(f"API call failed: {e}")
		raise


# Retry pattern with circuit breaker
async def process_with_retries_and_circuit_breaker(item, sem):
	failure_count = 0
	circuit_open = False

	for _ in range(RETRY_COUNT):
		try:
			async with sem:
				if circuit_open:
					logger.error("Circuit is open!")
					return None
				result = await do_api_call(item, await get_session())
				return result
		except Exception as e:
			failure_count += 1
			logger.error(f"API call failed: {e}")
			if failure_count > FAILURE_THRESHOLD:
				circuit_open = True
				logger.error("Circuit opened due to excessive failures")
				return None
			await asyncio.sleep(2 ** _)
	logger.error(f"All retries failed for item {item['id']}")
	return None


# Concurrent processing using semaphore
async def process_batch(items: List[Dict]):
	sem = asyncio.Semaphore(POOL_SIZE)
	tasks = [process_with_retries_and_circuit_breaker(item, sem) for item in items]
	return await asyncio.gather(*tasks)


def save_results(results):
	with open('results.json', 'w') as f:
		json.dump(results, f)
		logger.info("Results saved to file")


async def main():
	# Load configuration
	try:
		with open('config.json', 'r') as f:
			config = json.load(f)
			global POOL_SIZE, RETRY_COUNT, FAILURE_THRESHOLD
			POOL_SIZE = config.get('pool_size', POOL_SIZE)
			RETRY_COUNT = config.get('retry_count', RETRY_COUNT)
			FAILURE_THRESHOLD = config.get('failure_threshold', FAILURE_THRESHOLD)
	except FileNotFoundError:
		logger.warning("Config file not found, using defaults")
	except json.decoder.JSONDecodeError:
		logger.error("Invalid config file, using defaults")

	items = [{"id": i, "data": f"test_data_{i}"} for i in range(20)]

	try:
		results = await process_batch(items)
		results = [r for r in results if r]  # Remove None values
		if results:
			save_results(results)
			logger.info(f"Processed {len(results)} items")
		else:
			logger.warning("No items processed successfully")

	except Exception as e:
		logger.error(f"Batch processing failed: {e}")


if __name__ == "__main__":
	asyncio.run(main())
