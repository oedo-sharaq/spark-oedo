"""
Test the final GPU waveform fitter that avoids Arrow completely
"""

from pyspark.sql import SparkSession, functions as F
import numpy as np
from waveform.gpu_wavefitter import fit_waveforms_gpu
import time

def create_test_waveforms(spark, num_waveforms=100):
    """Create simple test waveforms"""
    waveforms = []
    ids = []
    
    print(f"Creating {num_waveforms} test waveforms...")
    
    for i in range(num_waveforms):
        # Simple pulse shape
        t = np.arange(256)
        baseline = 100
        amplitude = 200
        t0 = 128
        rise = 10
        decay = 30
        
        # Create pulse
        dt = t - t0
        pulse = baseline + amplitude * 0.5 * (1 + np.tanh(dt / rise)) * np.exp(-np.maximum(dt, 0) / decay)
        
        # Add some noise
        pulse += np.random.normal(0, 2, len(pulse))
        
        waveforms.append(pulse.tolist())
        ids.append(i)
    
    # Create DataFrame
    df = spark.createDataFrame(
        list(zip(ids, waveforms)), 
        ["id", "waveform"]
    )
    
    return df

def main():
    # Configure Spark to completely avoid Arrow
    spark = SparkSession.builder \
        .appName("TestGPUWaveformFitter") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1") \
        .getOrCreate()
    
    try:
        print("Creating test data...")
        df = create_test_waveforms(spark, num_waveforms=200)
        
        print(f"Created DataFrame with {df.count()} waveforms")
        df.show(5, truncate=False)
        
        print("\nStarting GPU waveform fitting...")
        start_time = time.time()
        
        # Fit waveforms
        results_df = fit_waveforms_gpu(
            df, 
            waveform_col="waveform",
            id_col="id",
            num_partitions=2
        )
        
        # Force execution
        print("Collecting results...")
        results = results_df.collect()
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\n=== Results ===")
        print(f"Processing time: {elapsed:.2f} seconds")
        print(f"Throughput: {len(results)/elapsed:.1f} waveforms/sec")
        print(f"Total results: {len(results)}")
        
        # Analyze results
        successful = sum(1 for r in results if r.ok)
        print(f"Successful fits: {successful}/{len(results)} ({100*successful/len(results):.1f}%)")
        
        if successful > 0:
            # Show statistics
            baselines = [r.baseline for r in results if r.ok]
            amplitudes = [r.amplitude for r in results if r.ok]
            t0s = [r.t0 for r in results if r.ok]
            chi2s = [r.chi2 for r in results if r.ok]
            
            print(f"\nParameter Statistics:")
            print(f"Baseline: {np.mean(baselines):.2f} ± {np.std(baselines):.2f}")
            print(f"Amplitude: {np.mean(amplitudes):.2f} ± {np.std(amplitudes):.2f}") 
            print(f"t0: {np.mean(t0s):.2f} ± {np.std(t0s):.2f}")
            print(f"Mean χ²: {np.mean(chi2s):.2f}")
        
        # Show first few results
        print(f"\nFirst 5 results:")
        for i, r in enumerate(results[:5]):
            print(f"ID {r.id}: baseline={r.baseline:.1f}, amp={r.amplitude:.1f}, "
                  f"t0={r.t0:.1f}, χ²={r.chi2:.1f}, ok={r.ok}")
        
        print("\n✓ Test completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()