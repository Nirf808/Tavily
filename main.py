import asyncio
from simulators import DataStore
from generator import DataGenerator
from orchestration.scaler import scaler_loop

async def main():
    print("[INIT] Starting Master Simulators...")
    ds = DataStore()

    gen = DataGenerator()
    gen.generate_seeds_and_init(ds)
    print(f"[INIT] Loaded {len(gen.master_urls)} seeds into memory.")

    asyncio.create_task(gen.domain_growth_loop(ds))
    await scaler_loop(ds)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Shutting down simulation cleanly...")
