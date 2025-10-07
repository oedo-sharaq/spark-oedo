from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame, SparkSession
import pandas as pd
import numpy as np
import torch
from typing import List, Tuple

# ----- Hyperparameters you can tune -----
MAX_ITERS = 120
LEARNING_RATE = 0.08
BATCH_SIZE = 512  # Smaller batch size for stability
SIGMA_FLOOR = 1e-3

POLARITY = 1  # +1 for positive pulses, -1 for negative pulses
BASELINE_RANGE = (0, 20)  # samples to use for baseline estimate
INITIAL_RISE = 8.0  # initial guess for pulse width
INITIAL_DECAY = 30.0  # initial guess for exponential decay time

ID_COL = "id"          
WF_COL = "waveform"    

# Schema for results
fit_schema = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("baseline", T.DoubleType()),
    T.StructField("amplitude", T.DoubleType()),
    T.StructField("t0", T.DoubleType()),
    T.StructField("rise", T.DoubleType()),
    T.StructField("decay", T.DoubleType()),
    T.StructField("chi2", T.DoubleType()),
    T.StructField("dof", T.IntegerType()),
    T.StructField("ok", T.BooleanType()),
    T.StructField("iters", T.IntegerType()),
])

def _initial_guesses(y_np: np.ndarray):
    """Generate initial parameter guesses for batch of waveforms"""
    B, L = y_np.shape
    
    # Baseline from first samples
    b0 = np.mean(y_np[:, BASELINE_RANGE[0]:BASELINE_RANGE[1]], axis=1)
    
    if POLARITY == -1:
        A0 = y_np.min(axis=1) - b0
        t0 = np.argmin(y_np, axis=1).astype(float)
    else:
        A0 = y_np.max(axis=1) - b0
        t0 = np.argmax(y_np, axis=1).astype(float)
    
    # Ensure reasonable initial values
    A0 = np.clip(A0, 1.0, 10000.0)  
    t0 = np.clip(t0, 10.0, L-10.0)  
    
    rise0 = np.full(B, INITIAL_RISE, dtype=float)
    decay0 = np.full(B, INITIAL_DECAY, dtype=float)
    
    return b0, A0, t0, rise0, decay0

def _fit_single_waveform(waveform: List[float], wf_id: int) -> Tuple:
    """
    Fit a single waveform using GPU acceleration.
    Returns tuple of fit parameters for RDD operations.
    """
    try:
        if not waveform or len(waveform) == 0:
            return (wf_id, 0.0, 0.0, 0.0, 0.0, 0.0, float('inf'), 1, False, 0)

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        # Convert to numpy and add batch dimension
        waveforms = np.array([waveform], dtype=np.float32)  # Shape: (1, L)
        B, L = waveforms.shape
        
        # Convert to torch tensors
        y = torch.from_numpy(waveforms).to(device)
        n = torch.arange(L, dtype=torch.float32, device=device)[None, :].expand(B, -1)
        
        # Initial parameter guesses
        b0, A0, t0, rise0, decay0 = _initial_guesses(waveforms.astype(np.float64))
        
        # Parameters (require gradients)
        b = torch.tensor(b0, device=device, dtype=torch.float32, requires_grad=True)
        A = torch.tensor(A0, device=device, dtype=torch.float32, requires_grad=True)
        t = torch.tensor(t0, device=device, dtype=torch.float32, requires_grad=True)
        r = torch.tensor(rise0, device=device, dtype=torch.float32, requires_grad=True)
        d = torch.tensor(decay0, device=device, dtype=torch.float32, requires_grad=True)
        
        # Optimizer
        optimizer = torch.optim.Adam([b, A, t, r, d], lr=LEARNING_RATE)
        
        # Model function
        def model():
            # Clamp parameters to reasonable ranges
            b_c = b  
            A_c = torch.clamp(A, min=0.1)  
            t_c = torch.clamp(t, min=0.0, max=float(L-1))  
            r_c = torch.clamp(r, min=0.1, max=float(L))  
            d_c = torch.clamp(d, min=0.1, max=float(L*10))  
            
            # Calculate model
            dt = n - t_c[:, None]
            rise_term = 0.5 * (1 + torch.erf(dt / r_c[:, None]))
            decay_term = torch.exp(-torch.clamp(dt, min=0.0) / d_c[:, None])
            
            return b_c[:, None] + A_c[:, None] * rise_term * decay_term
        
        # Fitting loop
        success = True
        
        for iteration in range(MAX_ITERS):
            optimizer.zero_grad()
            
            try:
                y_pred = model()
                residuals = y - y_pred
                loss = (residuals ** 2).mean()  
                
                if torch.isnan(loss) or torch.isinf(loss):
                    success = False
                    break
                    
                loss.backward()
                
                # Gradient clipping
                torch.nn.utils.clip_grad_norm_([b, A, t, r, d], max_norm=1.0)
                
                optimizer.step()
                
                # Early stopping
                if loss.item() < 1e-6:
                    break
                    
            except Exception as e:
                success = False
                break
        
        # Final evaluation
        try:
            with torch.no_grad():
                y_pred = model()
                residuals = y - y_pred
                chi2 = (residuals ** 2).sum(dim=1)
                dof = L - 5  # 5 parameters
        except:
            chi2 = torch.tensor([float('inf')], device=device)
            dof = 1
            success = False
        
        # Return as tuple for RDD operations
        return (
            int(wf_id),
            float(b.detach().cpu().numpy()[0]),
            float(A.detach().cpu().numpy()[0]),
            float(t.detach().cpu().numpy()[0]),
            float(r.detach().cpu().numpy()[0]),
            float(d.detach().cpu().numpy()[0]),
            float(chi2.detach().cpu().numpy()[0]),
            int(max(1, dof)),
            bool(success),
            int(iteration + 1)
        )
        
    except Exception as e:
        print(f"Error fitting waveform {wf_id}: {e}")
        return (int(wf_id), 0.0, 0.0, 0.0, 0.0, 0.0, float('inf'), 1, False, 0)

def _fit_partition_batch(partition_data: List) -> List[Tuple]:
    """
    Process a partition by batching waveforms for GPU efficiency.
    This processes multiple waveforms together on GPU.
    """
    if not partition_data:
        return []
    
    results = []
    
    # Process in batches for GPU efficiency
    for i in range(0, len(partition_data), BATCH_SIZE):
        batch = partition_data[i:i + BATCH_SIZE]
        
        # Extract waveforms and IDs
        waveforms = []
        ids = []
        
        for row in batch:
            ids.append(row[ID_COL])
            wf = row[WF_COL]
            if isinstance(wf, (list, tuple)):
                waveforms.append(list(wf))
            else:
                # Handle different array types
                waveforms.append([float(x) for x in wf])
        
        # Fit batch on GPU
        try:
            batch_results = _fit_waveform_batch_gpu(waveforms, ids)
            results.extend(batch_results)
        except Exception as e:
            print(f"Batch GPU fitting failed: {e}")
            # Fallback to single waveform fitting
            for wf, wf_id in zip(waveforms, ids):
                result = _fit_single_waveform(wf, wf_id)
                results.append(result)
    
    return results

def _fit_waveform_batch_gpu(waveforms: List[List[float]], ids: List[int]) -> List[Tuple]:
    """
    Fit a batch of waveforms on GPU and return as list of tuples
    """
    try:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        # Convert to numpy arrays
        max_len = max(len(wf) for wf in waveforms)
        
        # Pad waveforms to same length
        padded_waveforms = []
        for wf in waveforms:
            if len(wf) < max_len:
                padded = list(wf) + [wf[-1]] * (max_len - len(wf))  # Pad with last value
                padded_waveforms.append(padded)
            else:
                padded_waveforms.append(wf[:max_len])  # Truncate if needed
        
        wf_array = np.array(padded_waveforms, dtype=np.float32)
        B, L = wf_array.shape
        
        if B == 0:
            return []
        
        # Convert to torch tensors
        y = torch.from_numpy(wf_array).to(device)
        n = torch.arange(L, dtype=torch.float32, device=device)[None, :].expand(B, -1)
        
        # Initial parameter guesses
        b0, A0, t0, rise0, decay0 = _initial_guesses(wf_array.astype(np.float64))
        
        # Parameters (require gradients)
        b = torch.tensor(b0, device=device, dtype=torch.float32, requires_grad=True)
        A = torch.tensor(A0, device=device, dtype=torch.float32, requires_grad=True)
        t = torch.tensor(t0, device=device, dtype=torch.float32, requires_grad=True)
        r = torch.tensor(rise0, device=device, dtype=torch.float32, requires_grad=True)
        d = torch.tensor(decay0, device=device, dtype=torch.float32, requires_grad=True)
        
        # Optimizer
        optimizer = torch.optim.Adam([b, A, t, r, d], lr=LEARNING_RATE)
        
        # Model function
        def model():
            b_c = b  
            A_c = torch.clamp(A, min=0.1)  
            t_c = torch.clamp(t, min=0.0, max=float(L-1))  
            r_c = torch.clamp(r, min=0.1, max=float(L))  
            d_c = torch.clamp(d, min=0.1, max=float(L*10))  
            
            dt = n - t_c[:, None]
            rise_term = 0.5 * (1 + torch.erf(dt / r_c[:, None]))
            decay_term = torch.exp(-torch.clamp(dt, min=0.0) / d_c[:, None])
            
            return b_c[:, None] + A_c[:, None] * rise_term * decay_term
        
        # Fitting loop
        success = torch.ones(B, dtype=torch.bool, device=device)
        
        for iteration in range(MAX_ITERS):
            optimizer.zero_grad()
            
            try:
                y_pred = model()
                residuals = y - y_pred
                loss = (residuals ** 2).mean() 
                
                if torch.isnan(loss) or torch.isinf(loss):
                    success.fill_(False)
                    break
                    
                loss.backward()
                torch.nn.utils.clip_grad_norm_([b, A, t, r, d], max_norm=1.0)
                optimizer.step()
                
                if loss.item() < 1e-6:
                    break
                    
            except Exception as e:
                success.fill_(False)
                break
        
        # Final evaluation
        try:
            with torch.no_grad():
                y_pred = model()
                residuals = y - y_pred
                chi2 = (residuals ** 2).sum(dim=1)
                dof = L - 5  
        except:
            chi2 = torch.full((B,), float('inf'), device=device)
            dof = 1
            success.fill_(False)
        
        # Convert to list of tuples
        results = []
        for i in range(B):
            results.append((
                int(ids[i]),
                float(b[i].detach().cpu().numpy()),
                float(A[i].detach().cpu().numpy()),
                float(t[i].detach().cpu().numpy()),
                float(r[i].detach().cpu().numpy()),
                float(d[i].detach().cpu().numpy()),
                float(chi2[i].detach().cpu().numpy()),
                int(max(1, dof)),
                bool(success[i].detach().cpu().numpy()),
                int(iteration + 1)
            ))
        
        return results
        
    except Exception as e:
        print(f"GPU batch fitting failed: {e}")
        # Fallback to individual fitting
        results = []
        for wf, wf_id in zip(waveforms, ids):
            result = _fit_single_waveform(wf, wf_id)
            results.append(result)
        return results

def fitWaveforms(df: DataFrame, num_partitions: int = 8) -> DataFrame:
    """
    Fit waveforms using GPU acceleration with pure RDD approach (no Arrow)
    """
    # Ensure we have required columns
    if ID_COL not in df.columns:
        df = df.withColumn(ID_COL, F.monotonically_increasing_id())
    
    # Select required columns and repartition
    df_prepared = df.select(ID_COL, WF_COL).repartition(num_partitions)
    
    print("Using RDD-based GPU fitting (no Arrow serialization)")
    
    # Convert to RDD and process partitions
    def process_partition(iterator):
        # Collect all rows in the partition
        partition_data = list(iterator)
        if not partition_data:
            return iter([])
        
        # Process the partition with GPU batching
        results = _fit_partition_batch(partition_data)
        return iter(results)
    
    # Process using RDD operations
    rdd_results = df_prepared.rdd.mapPartitions(process_partition)
    
    # Convert back to DataFrame
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result_df = spark.createDataFrame(rdd_results, schema=fit_schema)
    
    return result_df

# Convenience function
def fit_waveforms_gpu(spark_df: DataFrame, 
                     waveform_col: str = "waveform", 
                     id_col: str = "id",
                     num_partitions: int = 8) -> DataFrame:
    """
    Convenience function to fit waveforms with custom column names
    """
    global ID_COL, WF_COL
    ID_COL = id_col
    WF_COL = waveform_col
    
    return fitWaveforms(spark_df, num_partitions)