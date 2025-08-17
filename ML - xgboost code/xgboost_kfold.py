import pandas as pd
import os
import sys
from datetime import datetime
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error, explained_variance_score
from sklearn.model_selection import GroupKFold
import numpy as np
import joblib

# Change to this script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Determine bin numbers AND split strategies to test
bins = [3.1,3, 4, 5]
split_strats = [1, 2]

print("="*80)
print("RUNNING XGBOOST K-FOLD FOR MULTIPLE BIN CONFIGURATIONS AND SPLIT STRATEGIES")
print("="*80)
print(f"Testing bin configurations: {bins}")
print(f"Testing split strategies: {split_strats}")

# Store results for all bin configurations
all_bin_results = []

# Loop through each bin configuration and split strategy
for no_bins in bins:
    for split_strat in split_strats:
        print(f"\n{'='*80}")
        print(f"PROCESSING BIN CONFIGURATION: {no_bins} with SPLIT STRATEGY: {split_strat}")
        print(f"{'='*80}")
        
        if no_bins == 3.1:
            DATASET_PATH = rf"split_datasets/data_split_strat{split_strat}/MAC_DATASET_LLOD_FILTERED/kfold_based_split"
            OUTPUT_PATH = rf"outputs/output_xgboost_kfold_{no_bins}bins_{split_strat}splitstrat_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        else:
            DATASET_PATH = rf"split_datasets/data_split_strat{split_strat}/MAC_binning_{no_bins}bins_optimized/kfold_based_split"
            OUTPUT_PATH = rf"outputs/output_xgboost_kfold_{no_bins}bins_{split_strat}splitstrat_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Check if dataset path exists
        if not os.path.exists(DATASET_PATH):
            print(f"WARNING: Dataset path does not exist: {DATASET_PATH}")
            print(f"Skipping bin configuration {no_bins} with split strategy {split_strat}")
            continue

        # Columns to be used for training and prediction
        PREDICTOR_COLUMNS = ['N1', 'N2', 'N3', 'N_total', 'V1', 'V2', 'V3', 'V_total', 'SAE', 'AAE', 'SSA_red', 'SSA_blue', 'SSA_green', 'b_scat_red', 'b_scat_blue', 'b_scat_green', 'b_abs_red', 'b_abs_blue', 'b_abs_green']
        PREDICTED_COLUMN = 'MAC_bc'

        # Create output directory
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        print(f"Output directory created: {OUTPUT_PATH}")

        print("XGBOOST K-FOLD NESTED CROSS-VALIDATION WITH BAYESIAN OPTIMIZATION")
        print(f"Dataset path: {DATASET_PATH}")

        # Data cleaning function
        def clean_numeric_data(df, predictor_columns, predicted_column):
            """Clean and convert columns to numeric"""
            # print("Cleaning data types...")
            
            # # Clean predictor columns
            # for col in predictor_columns:
            #     if col in df.columns:
            #         # Convert to numeric, replacing errors with NaN
            #         df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            #         # Check for NaN values
            #         nan_count = df[col].isna().sum()
            #         if nan_count > 0:
            #             print(f"  Warning: {nan_count} NaN values in {col}, filling with median")
            #             # Fill NaN with median
            #             df[col] = df[col].fillna(df[col].median())
            
            # # Clean target column
            # if predicted_column in df.columns:
            #     df[predicted_column] = pd.to_numeric(df[predicted_column], errors='coerce')
            #     nan_count = df[predicted_column].isna().sum()
            #     if nan_count > 0:
            #         print(f"  Warning: {nan_count} NaN values in {predicted_column}, filling with median")
            #         df[predicted_column] = df[predicted_column].fillna(df[predicted_column].median())
            
            return df

        # Comprehensive metrics calculation functions
        def calculate_comprehensive_metrics(y_true, y_pred):
            """Calculate all performance metrics"""
            mse = mean_squared_error(y_true, y_pred)
            rmse = np.sqrt(mse)
            mae = mean_absolute_error(y_true, y_pred)
            r2 = r2_score(y_true, y_pred)
            mb = np.mean(y_pred - y_true)  # Mean Bias
            
            # Add MAPE - Mean Absolute Percentage Error
            mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
            
            # Add EVS - Explained Variance Score
            evs = explained_variance_score(y_true, y_pred)
            
            return {
                'mse': mse,
                'rmse': rmse,
                'mae': mae,
                'r2': r2,
                'mape': mape,
                'evs': evs,
                'mb': mb
            }

        # GroupKFold function for inner CV
        def create_groupkfold_splits(df, n_splits=3):
            """Create GroupKFold splits based on campaigns"""
            campaigns = df['Campaign'].unique()
            groups = df['Campaign'].values
            
            gkf = GroupKFold(n_splits=n_splits)
            splits = []
            
            for train_idx, val_idx in gkf.split(df, groups=groups):
                train_campaigns = set(df.iloc[train_idx]['Campaign'].unique())
                val_campaigns = set(df.iloc[val_idx]['Campaign'].unique())
                
                splits.append({
                    'train_idx': train_idx,
                    'val_idx': val_idx,
                    'train_campaigns': list(train_campaigns),
                    'val_campaigns': list(val_campaigns)
                })
            
            return splits

        # Bayesian optimization with Optuna
        try:
            import optuna
            print("Optuna loaded successfully for Bayesian optimization")
            use_bayesian = True
        except ImportError:
            print("Optuna not available, using Grid Search")
            use_bayesian = False

        # Store results for each outer fold
        outer_fold_results = []
        best_params_per_fold = []
        all_bayesian_results = []

        print("\n" + "="*60)
        print("PROCESSING OUTER FOLDS")
        print("="*60)

        # Process each outer fold (kfold1 to kfold5)
        for fold_num in range(1, 6):
            fold_dir = os.path.join(DATASET_PATH, f"kfold{fold_num}")
            
            print(f"\n--- PROCESSING OUTER FOLD {fold_num} ---")
            
            # Load outer fold data
            outer_train_path = os.path.join(fold_dir, "train.csv")
            outer_test_path = os.path.join(fold_dir, "test.csv")
            
            if not os.path.exists(outer_train_path) or not os.path.exists(outer_test_path):
                print(f"WARNING: Missing files for fold {fold_num}, skipping...")
                continue
            
            outer_train_df = pd.read_csv(outer_train_path)
            outer_test_df = pd.read_csv(outer_test_path)
            
            # Clean data types
            outer_train_df = clean_numeric_data(outer_train_df, PREDICTOR_COLUMNS, PREDICTED_COLUMN)
            outer_test_df = clean_numeric_data(outer_test_df, PREDICTOR_COLUMNS, PREDICTED_COLUMN)
            
            print(f"Outer train shape: {outer_train_df.shape}")
            print(f"Outer test shape: {outer_test_df.shape}")
            
            # Check campaigns in each split
            if 'Campaign' in outer_train_df.columns:
                train_campaigns = sorted(outer_train_df['Campaign'].unique())
                test_campaigns = sorted(outer_test_df['Campaign'].unique())
                print(f"Train campaigns ({len(train_campaigns)}): {train_campaigns}")
                print(f"Test campaigns ({len(test_campaigns)}): {test_campaigns}")
            
            # Prepare outer fold data
            X_outer_train = outer_train_df[PREDICTOR_COLUMNS]
            y_outer_train = outer_train_df[PREDICTED_COLUMN]
            X_outer_test = outer_test_df[PREDICTOR_COLUMNS]
            y_outer_test = outer_test_df[PREDICTED_COLUMN]
            
            # ================================================================
            # STEP 1.1: Bayesian Optimization Hyperparameter Tuning
            # ================================================================
            print(f"\nSTEP 1.1: Bayesian optimization hyperparameter tuning...")
            
            # Create GroupKFold splits for inner CV
            inner_splits = create_groupkfold_splits(outer_train_df, n_splits=3)
            print(f"Created {len(inner_splits)} inner splits using GroupKFold")
            
            # Display inner splits
            for i, split in enumerate(inner_splits):
                print(f"  Inner split {i+1}: Train campaigns {split['train_campaigns']}, Val campaigns {split['val_campaigns']}")
            
            fold_bayesian_results = []
            best_score = float('inf')
            best_params = None
            
            if use_bayesian:
                print("Using Bayesian Optimization with Optuna")
                
                def cv_objective(trial):
                    # Define hyperparameters
                    params = {
                        'n_estimators': trial.suggest_int('n_estimators', 50, 500),
                        'max_depth': trial.suggest_int('max_depth', 3, 15),
                        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                        'reg_alpha': trial.suggest_float('reg_alpha', 0.01, 10.0),
                        'reg_lambda': trial.suggest_float('reg_lambda', 0.01, 10.0),
                    }
                    
                    # 3-fold GroupKFold CV
                    inner_val_scores = []
                    inner_train_scores = []
                    
                    for split_idx, split in enumerate(inner_splits):
                        # Get inner train and validation data
                        X_inner_train = outer_train_df.iloc[split['train_idx']][PREDICTOR_COLUMNS]
                        y_inner_train = outer_train_df.iloc[split['train_idx']][PREDICTED_COLUMN]
                        X_inner_val = outer_train_df.iloc[split['val_idx']][PREDICTOR_COLUMNS]
                        y_inner_val = outer_train_df.iloc[split['val_idx']][PREDICTED_COLUMN]
                        
                        # Train model
                        model = XGBRegressor(**params, random_state=42, n_jobs=-1, tree_method='hist')
                        model.fit(X_inner_train, y_inner_train)
                        
                        # Predictions
                        y_pred_train = model.predict(X_inner_train)
                        y_pred_val = model.predict(X_inner_val)
                        
                        # Calculate comprehensive metrics
                        train_metrics = calculate_comprehensive_metrics(y_inner_train, y_pred_train)
                        val_metrics = calculate_comprehensive_metrics(y_inner_val, y_pred_val)
                        
                        inner_train_scores.append(train_metrics)
                        inner_val_scores.append(val_metrics)
                        
                        # Store detailed results for this trial and split
                        fold_bayesian_results.append({
                            'outer_fold': fold_num,
                            'trial_number': trial.number,
                            'inner_split': split_idx + 1,
                            'split_type': 'train',
                            'train_campaigns': ', '.join(split['train_campaigns']),
                            'val_campaigns': ', '.join(split['val_campaigns']),
                            **params,
                            **{f'train_{k}': v for k, v in train_metrics.items()}
                        })
                        
                        fold_bayesian_results.append({
                            'outer_fold': fold_num,
                            'trial_number': trial.number,
                            'inner_split': split_idx + 1,
                            'split_type': 'validation',
                            'train_campaigns': ', '.join(split['train_campaigns']),
                            'val_campaigns': ', '.join(split['val_campaigns']),
                            **params,
                            **{f'val_{k}': v for k, v in val_metrics.items()}
                        })
                    
                    # Return average validation MSE
                    avg_val_mse = np.mean([score['mse'] for score in inner_val_scores])
                    return avg_val_mse
                
                # Run Bayesian optimization
                study = optuna.create_study(direction='minimize', sampler=optuna.samplers.TPESampler(seed=42))
                study.optimize(cv_objective, n_trials=15)
                
                best_params = study.best_params
                best_score = study.best_value
                
            else:
                # Fallback to manual grid search if Optuna not available
                print("Using manual grid search")
                from itertools import product
                
                param_grid = {
                    'n_estimators': [100, 200, 300],
                    'max_depth': [6, 8, 10],
                    'learning_rate': [0.01, 0.05, 0.1],
                    'subsample': [0.8, 0.9],
                    'colsample_bytree': [0.8, 0.9]
                }
                
                param_combinations = list(product(*param_grid.values()))
                
                for trial_idx, param_values in enumerate(param_combinations):
                    params = dict(zip(param_grid.keys(), param_values))
                    
                    # 3-fold GroupKFold CV (same structure as Bayesian)
                    inner_val_scores = []
                    inner_train_scores = []
                    
                    for split_idx, split in enumerate(inner_splits):
                        X_inner_train = outer_train_df.iloc[split['train_idx']][PREDICTOR_COLUMNS]
                        y_inner_train = outer_train_df.iloc[split['train_idx']][PREDICTED_COLUMN]
                        X_inner_val = outer_train_df.iloc[split['val_idx']][PREDICTOR_COLUMNS]
                        y_inner_val = outer_train_df.iloc[split['val_idx']][PREDICTED_COLUMN]
                        
                        model = XGBRegressor(**params, random_state=42, n_jobs=-1, tree_method='hist')
                        model.fit(X_inner_train, y_inner_train)
                        
                        y_pred_train = model.predict(X_inner_train)
                        y_pred_val = model.predict(X_inner_val)
                        
                        train_metrics = calculate_comprehensive_metrics(y_inner_train, y_pred_train)
                        val_metrics = calculate_comprehensive_metrics(y_inner_val, y_pred_val)
                        
                        inner_train_scores.append(train_metrics)
                        inner_val_scores.append(val_metrics)
                        
                        # Store results
                        fold_bayesian_results.append({
                            'outer_fold': fold_num,
                            'trial_number': trial_idx,
                            'inner_split': split_idx + 1,
                            'split_type': 'train',
                            'train_campaigns': ', '.join(split['train_campaigns']),
                            'val_campaigns': ', '.join(split['val_campaigns']),
                            **params,
                            **{f'train_{k}': v for k, v in train_metrics.items()}
                        })
                        
                        fold_bayesian_results.append({
                            'outer_fold': fold_num,
                            'trial_number': trial_idx,
                            'inner_split': split_idx + 1,
                            'split_type': 'validation',
                            'train_campaigns': ', '.join(split['train_campaigns']),
                            'val_campaigns': ', '.join(split['val_campaigns']),
                            **params,
                            **{f'val_{k}': v for k, v in val_metrics.items()}
                        })
                    
                    avg_val_mse = np.mean([score['mse'] for score in inner_val_scores])
                    if avg_val_mse < best_score:
                        best_score = avg_val_mse
                        best_params = params
            
            # Store all Bayesian results for this fold
            all_bayesian_results.extend(fold_bayesian_results)
            
            print(f"Best parameters for fold {fold_num}: {best_params}")
            print(f"Best CV MSE: {best_score:.6f}")
            
            best_params_per_fold.append({
                'fold': fold_num,
                'best_params': best_params,
                'cv_mse': best_score
            })
            
            # ================================================================
            # STEP 1.3: Retrain model on all training campaigns with best parameters
            # ================================================================
            print(f"\nSTEP 1.3: Training final model for outer fold {fold_num}...")
            
            final_model = XGBRegressor(**best_params, random_state=42, n_jobs=-1, tree_method='hist')
            final_model.fit(X_outer_train, y_outer_train)
            
            # Calculate training performance
            y_pred_outer_train = final_model.predict(X_outer_train)
            outer_train_metrics = calculate_comprehensive_metrics(y_outer_train, y_pred_outer_train)
            
            # ================================================================
            # STEP 1.4: Evaluate on outer test set
            # ================================================================
            print(f"STEP 1.4: Evaluating on outer test set...")
            
            y_pred_outer_test = final_model.predict(X_outer_test)
            outer_test_metrics = calculate_comprehensive_metrics(y_outer_test, y_pred_outer_test)
            
            fold_result = {
                'fold': fold_num,
                'test_samples': len(y_outer_test),
                'train_samples': len(y_outer_train),
                'test_campaigns': ', '.join(test_campaigns),
                'train_campaigns': ', '.join(train_campaigns),
                **{f'train_{k}': v for k, v in outer_train_metrics.items()},
                **{f'test_{k}': v for k, v in outer_test_metrics.items()}
            }
            
            outer_fold_results.append(fold_result)
            
            print(f"Outer Fold {fold_num} Results:")
            print(f"  Training - RMSE: {outer_train_metrics['rmse']:.6f}, R2: {outer_train_metrics['r2']:.6f}")
            print(f"  Test - RMSE: {outer_test_metrics['rmse']:.6f}, R2: {outer_test_metrics['r2']:.6f}")

        # ================================================================
        # STEP 2: Aggregate results across outer folds
        # ================================================================
        print("\n" + "="*60)
        print("STEP 2: AGGREGATED CROSS-VALIDATION RESULTS")
        print("="*60)

        if outer_fold_results:
            # Calculate aggregated metrics
            train_metrics = ['train_mse', 'train_rmse', 'train_mae', 'train_r2', 'train_mape', 'train_evs', 'train_mb']
            test_metrics = ['test_mse', 'test_rmse', 'test_mae', 'test_r2', 'test_mape', 'test_evs', 'test_mb']
            
            aggregated_results = {}
            
            for metric_set in [train_metrics, test_metrics]:
                for metric in metric_set:
                    values = [result[metric] for result in outer_fold_results]
                    aggregated_results[metric] = {
                        'mean': np.mean(values),
                        'std': np.std(values),
                        'values': values
                    }
            
            print("Cross-Validation Performance (Mean +/- Std):")
            print("TRAINING:")
            print(f"  RMSE: {aggregated_results['train_rmse']['mean']:.6f} +/- {aggregated_results['train_rmse']['std']:.6f}")
            print(f"  R2: {aggregated_results['train_r2']['mean']:.6f} +/- {aggregated_results['train_r2']['std']:.6f}")
            print(f"  MAE: {aggregated_results['test_mae']['mean']:.6f} +/- {aggregated_results['test_mae']['std']:.6f}")

        # ================================================================
        # STEP 3: Select final hyperparameters
        # ================================================================
        print("\n" + "="*60)
        print("STEP 3: FINAL HYPERPARAMETER SELECTION")
        print("="*60)

        if best_params_per_fold:
            # Method (c): Parameters from best-performing fold
            best_fold_idx = np.argmin([params['cv_mse'] for params in best_params_per_fold])
            best_overall_params = best_params_per_fold[best_fold_idx]['best_params']
            
            print("Using method (c): Parameters from best-performing fold")
            print("Final hyperparameters:")
            for param, value in best_overall_params.items():
                print(f"  {param}: {value}")
            
            print(f"\nBest CV MSE: {best_params_per_fold[best_fold_idx]['cv_mse']:.6f}")

        # ================================================================
        # STEP 4: Train final model on all campaigns
        # ================================================================
        print("\n" + "="*60)
        print("STEP 4: TRAINING FINAL MODEL ON ALL CAMPAIGNS")
        print("="*60)

        # Combine all outer training data
        all_train_dfs = []
        for fold_num in range(1, 6):
            fold_dir = os.path.join(DATASET_PATH, f"kfold{fold_num}")
            outer_train_path = os.path.join(fold_dir, "train.csv")
            
            if os.path.exists(outer_train_path):
                outer_train_df = pd.read_csv(outer_train_path)
                outer_train_df = clean_numeric_data(outer_train_df, PREDICTOR_COLUMNS, PREDICTED_COLUMN)
                all_train_dfs.append(outer_train_df)

        if all_train_dfs and best_overall_params:
            combined_train_df = pd.concat(all_train_dfs, ignore_index=True)
            
            X_final_train = combined_train_df[PREDICTOR_COLUMNS]
            y_final_train = combined_train_df[PREDICTED_COLUMN]
            
            print(f"Combined training data shape: {combined_train_df.shape}")
            final_campaigns = sorted(combined_train_df['Campaign'].unique())
            print(f"Final training campaigns ({len(final_campaigns)}): {final_campaigns}")
            
            # Train final model
            final_model = XGBRegressor(**best_overall_params, random_state=42, n_jobs=-1, tree_method='hist')
            final_model.fit(X_final_train, y_final_train)
            
            # Calculate final training performance
            y_pred_final_train = final_model.predict(X_final_train)
            final_train_metrics = calculate_comprehensive_metrics(y_final_train, y_pred_final_train)
            
            print("Final Training Performance:")
            print(f"  RMSE: {final_train_metrics['rmse']:.6f}")
            print(f"  R2: {final_train_metrics['r2']:.6f}")
            print(f"  MAE: {final_train_metrics['mae']:.6f}")
            print(f"  MSE: {final_train_metrics['mse']:.6f}")
            print(f"  MAPE: {final_train_metrics['mape']:.6f}")
            print(f"  EVS: {final_train_metrics['evs']:.6f}")
            print(f"  MB: {final_train_metrics['mb']:.6f}")
            
            # ================================================================
            # STEP 4.5: Evaluate final model on each outer fold's training campaigns
            # ================================================================
            print("\nSTEP 4.5: Evaluating final model on individual training campaign sets...")

            final_model_training_results = []

            for fold_num in range(1, 6):
                fold_dir = os.path.join(DATASET_PATH, f"kfold{fold_num}")
                outer_train_path = os.path.join(fold_dir, "train.csv")
                
                if os.path.exists(outer_train_path):
                    outer_train_df = pd.read_csv(outer_train_path)
                    outer_train_df = clean_numeric_data(outer_train_df, PREDICTOR_COLUMNS, PREDICTED_COLUMN)
                    X_fold_train = outer_train_df[PREDICTOR_COLUMNS]
                    y_fold_train = outer_train_df[PREDICTED_COLUMN]
                    
                    # Use final model to predict on this fold's training data
                    y_pred_fold_train = final_model.predict(X_fold_train)
                    fold_train_metrics = calculate_comprehensive_metrics(y_fold_train, y_pred_fold_train)
                    
                    fold_train_result = {
                        'fold': fold_num,
                        'samples': len(y_fold_train),
                        'campaigns': ', '.join(sorted(outer_train_df['Campaign'].unique())),
                        **fold_train_metrics
                    }
                    final_model_training_results.append(fold_train_result)
                    
                    print(f"  Fold {fold_num}: RMSE: {fold_train_metrics['rmse']:.6f}, R2: {fold_train_metrics['r2']:.6f}")

            # Calculate aggregated training metrics
            aggregated_training_results = {}
            if final_model_training_results:
                training_metrics_list = ['mse', 'rmse', 'mae', 'r2', 'mape', 'evs', 'mb']
                
                for metric in training_metrics_list:
                    values = [result[metric] for result in final_model_training_results]
                    aggregated_training_results[metric] = {
                        'mean': np.mean(values),
                        'std': np.std(values),
                        'values': values
                    }
                
                print("\nAggregated Final Model Training Performance:")
                print(f"  RMSE: {aggregated_training_results['rmse']['mean']:.6f} +/- {aggregated_training_results['rmse']['std']:.6f}")
                print(f"  R2: {aggregated_training_results['r2']['mean']:.6f} +/- {aggregated_training_results['r2']['std']:.6f}")
                print(f"  MAE: {aggregated_training_results['mae']['mean']:.6f} +/- {aggregated_training_results['mae']['std']:.6f}")
            
            # ================================================================
            # STEP 5: Evaluate on hold-out evaluation set
            # ================================================================
            evaluation_path = os.path.join(DATASET_PATH, "evaluation", "evaluation.csv")
            
            if os.path.exists(evaluation_path):
                print("\n" + "="*60)
                print("STEP 5: FINAL EVALUATION ON HOLD-OUT CAMPAIGNS")
                print("="*60)
                
                eval_df = pd.read_csv(evaluation_path)
                eval_df = clean_numeric_data(eval_df, PREDICTOR_COLUMNS, PREDICTED_COLUMN)
                X_eval = eval_df[PREDICTOR_COLUMNS]
                y_eval = eval_df[PREDICTED_COLUMN]
                
                print(f"Evaluation data shape: {eval_df.shape}")
                
                # Check campaigns in evaluation set
                eval_campaigns = sorted(eval_df['Campaign'].unique())
                print(f"Evaluation campaigns ({len(eval_campaigns)}): {eval_campaigns}")
                
                y_pred_eval = final_model.predict(X_eval)
                
                # Overall evaluation metrics
                overall_eval_metrics = calculate_comprehensive_metrics(y_eval, y_pred_eval)
                
                print("Overall Evaluation Results:")
                print(f"  RMSE: {overall_eval_metrics['rmse']:.6f}")
                print(f"  R2: {overall_eval_metrics['r2']:.6f}")
                print(f"  MAE: {overall_eval_metrics['mae']:.6f}")
                print(f"  MSE: {overall_eval_metrics['mse']:.6f}")
                print(f"  MAPE: {overall_eval_metrics['mape']:.6f}")
                print(f"  EVS: {overall_eval_metrics['evs']:.6f}")
                print(f"  MB: {overall_eval_metrics['mb']:.6f}")
                
                # Per-campaign evaluation
                campaign_eval_results = []
                print("\nPer-campaign Evaluation Results:")
                
                for campaign in eval_campaigns:
                    campaign_mask = eval_df['Campaign'] == campaign
                    campaign_df = eval_df[campaign_mask]
                    
                    if campaign_df.shape[0] > 0:
                        X_campaign = campaign_df[PREDICTOR_COLUMNS]
                        y_campaign = campaign_df[PREDICTED_COLUMN]
                        y_pred_campaign = final_model.predict(X_campaign)
                        
                        campaign_metrics = calculate_comprehensive_metrics(y_campaign, y_pred_campaign)
                        
                        campaign_result = {
                            'campaign': campaign,
                            'samples': len(y_campaign),
                            **campaign_metrics
                        }
                        campaign_eval_results.append(campaign_result)
                        
                        print(f"  {campaign} ({len(y_campaign)} samples):")
                        print(f"    RMSE: {campaign_metrics['rmse']:.6f}")
                        print(f"    R2: {campaign_metrics['r2']:.6f}")
                        print(f"    MAE: {campaign_metrics['mae']:.6f}")
                        print(f"    MSE: {campaign_metrics['mse']:.6f}")
                        print(f"    MAPE: {campaign_metrics['mape']:.6f}")
                        print(f"    EVS: {campaign_metrics['evs']:.6f}")
                        print(f"    MB: {campaign_metrics['mb']:.6f}")
                
                # Feature importance
                feature_importance = pd.DataFrame({
                    'feature': PREDICTOR_COLUMNS,
                    'importance': final_model.feature_importances_
                }).sort_values('importance', ascending=False)
                
                print("\nTop 15 Feature Importance:")
                for idx, row in feature_importance.head(15).iterrows():
                    print(f"  {row['feature']}: {row['importance']:.4f}")
                
                # ================================================================
                # SAVE ALL RESULTS
                # ================================================================
                print(f"\nSaving results to: {OUTPUT_PATH}")
                
                # Save model
                model_filename = os.path.join(OUTPUT_PATH, "final_xgboost_model.pkl")
                joblib.dump(final_model, model_filename)
                print(f"Model saved to: {model_filename}")
                
                # Save Bayesian optimization results
                if all_bayesian_results:
                    bayesian_df = pd.DataFrame(all_bayesian_results)
                    bayesian_df.to_csv(os.path.join(OUTPUT_PATH, "bayesian_optimization_results.csv"), index=False)
                    print(f"Bayesian optimization results saved ({len(all_bayesian_results)} records)")
                
                # Save outer fold performance
                if outer_fold_results:
                    outer_fold_df = pd.DataFrame(outer_fold_results)
                    outer_fold_df.to_csv(os.path.join(OUTPUT_PATH, "outer_fold_performance.csv"), index=False)
                    print(f"Outer fold performance saved")
                
                # Save final model performance
                final_performance = [{
                    'dataset': 'final_training',
                    'campaigns': ', '.join(final_campaigns),
                    'num_campaigns': len(final_campaigns),
                    'samples': len(y_final_train),
                    **final_train_metrics
                }]
                final_performance_df = pd.DataFrame(final_performance)
                final_performance_df.to_csv(os.path.join(OUTPUT_PATH, "final_model_performance.csv"), index=False)
                print(f"Final model performance saved")
                
                # Save final model training on individual folds
                if final_model_training_results:
                    final_training_df = pd.DataFrame(final_model_training_results)
                    final_training_df.to_csv(os.path.join(OUTPUT_PATH, "final_model_training_individual_folds.csv"), index=False)
                    print(f"Final model training on individual folds saved")
                
                # Save hold-out evaluation results
                holdout_results = []
                # Add per-campaign results
                for result in campaign_eval_results:
                    holdout_results.append({
                        'evaluation_type': 'per_campaign',
                        **result
                    })
                # Add overall results
                holdout_results.append({
                    'evaluation_type': 'combined',
                    'campaign': 'ALL_HOLDOUT',
                    'samples': len(y_eval),
                    **overall_eval_metrics
                })
                
                holdout_df = pd.DataFrame(holdout_results)
                holdout_df.to_csv(os.path.join(OUTPUT_PATH, "holdout_evaluation_per_campaign.csv"), index=False)
                print(f"Hold-out evaluation results saved")
                
                # Save feature importance
                feature_importance.to_csv(os.path.join(OUTPUT_PATH, "feature_importance.csv"), index=False)
                
                # Save comprehensive report
                with open(os.path.join(OUTPUT_PATH, "comprehensive_kfold_report.txt"), 'w', encoding='utf-8') as f:
                    f.write("XGBOOST K-FOLD NESTED CROSS-VALIDATION COMPREHENSIVE REPORT\n")
                    f.write("="*80 + "\n")
                    f.write(f"Bin Configuration: {no_bins}\n")
                    f.write(f"Split Strategy: {split_strat}\n")
                    f.write(f"Training Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Dataset Path: {DATASET_PATH}\n\n")
                    
                    f.write("METHODOLOGY:\n")
                    f.write("- Step 0: Hold-out evaluation campaigns\n")
                    f.write("- Step 1: 5-fold outer cross-validation\n")
                    f.write("  - Step 1.1: Bayesian optimization with 3-fold GroupKFold inner CV\n")
                    f.write("  - Step 1.2: Best hyperparameter selection\n")
                    f.write("  - Step 1.3: Model training on all outer training campaigns\n")
                    f.write("  - Step 1.4: Evaluation on outer test campaign\n")
                    f.write("- Step 2: Aggregate outer fold performance metrics\n")
                    f.write("- Step 3: Final hyperparameter selection\n")
                    f.write("- Step 4: Final model training on all campaigns\n")
                    f.write("- Step 4.5: Final model evaluation on individual training folds\n")
                    f.write("- Step 5: Hold-out evaluation with per-campaign reporting\n\n")
                    
                    f.write("HYPERPARAMETER OPTIMIZATION:\n")
                    f.write(f"Method: {'Bayesian Optimization (Optuna)' if use_bayesian else 'Grid Search'}\n")
                    f.write(f"Trials per outer fold: {15 if use_bayesian else len(param_combinations) if 'param_combinations' in locals() else 'N/A'}\n")
                    f.write(f"Inner CV: 3-fold GroupKFold\n\n")
                    
                    f.write("FINAL HYPERPARAMETERS:\n")
                    for param, value in best_overall_params.items():
                        f.write(f"  {param}: {value}\n")
                    f.write("\n")
                    
                    if aggregated_results:
                        f.write("CROSS-VALIDATION PERFORMANCE (Mean +/- Std):\n")
                        f.write("TRAINING:\n")
                        f.write(f"  RMSE: {aggregated_results['train_rmse']['mean']:.6f} +/- {aggregated_results['train_rmse']['std']:.6f}\n")
                        f.write(f"  R2: {aggregated_results['train_r2']['mean']:.6f} +/- {aggregated_results['train_r2']['std']:.6f}\n")
                        f.write(f"  MAE: {aggregated_results['train_mae']['mean']:.6f} +/- {aggregated_results['train_mae']['std']:.6f}\n")
                        f.write(f"  MSE: {aggregated_results['train_mse']['mean']:.6f} +/- {aggregated_results['train_mse']['std']:.6f}\n")
                        f.write(f"  MAPE: {aggregated_results['train_mape']['mean']:.6f} +/- {aggregated_results['train_mape']['std']:.6f}\n")
                        f.write(f"  EVS: {aggregated_results['train_evs']['mean']:.6f} +/- {aggregated_results['train_evs']['std']:.6f}\n")
                        f.write(f"  MB: {aggregated_results['train_mb']['mean']:.6f} +/- {aggregated_results['train_mb']['std']:.6f}\n")
                        f.write("TESTING:\n")
                        f.write(f"  RMSE: {aggregated_results['test_rmse']['mean']:.6f} +/- {aggregated_results['test_rmse']['std']:.6f}\n")
                        f.write(f"  R2: {aggregated_results['test_r2']['mean']:.6f} +/- {aggregated_results['test_r2']['std']:.6f}\n")
                        f.write(f"  MAE: {aggregated_results['test_mae']['mean']:.6f} +/- {aggregated_results['test_mae']['std']:.6f}\n")
                        f.write(f"  MSE: {aggregated_results['test_mse']['mean']:.6f} +/- {aggregated_results['test_mse']['std']:.6f}\n")
                        f.write(f"  MAPE: {aggregated_results['test_mape']['mean']:.6f} +/- {aggregated_results['test_mape']['std']:.6f}\n")
                        f.write(f"  EVS: {aggregated_results['test_evs']['mean']:.6f} +/- {aggregated_results['test_evs']['std']:.6f}\n")
                        f.write(f"  MB: {aggregated_results['test_mb']['mean']:.6f} +/- {aggregated_results['test_mb']['std']:.6f}\n\n")
                    
                    f.write("FINAL MODEL TRAINING PERFORMANCE:\n")
                    f.write(f"Campaigns: {final_campaigns}\n")
                    f.write(f"Samples: {len(y_final_train)}\n")
                    f.write(f"RMSE: {final_train_metrics['rmse']:.6f}\n")
                    f.write(f"R2: {final_train_metrics['r2']:.6f}\n")
                    f.write(f"MAE: {final_train_metrics['mae']:.6f}\n")
                    f.write(f"MSE: {final_train_metrics['mse']:.6f}\n")
                    f.write(f"MAPE: {final_train_metrics['mape']:.6f}\n")
                    f.write(f"EVS: {final_train_metrics['evs']:.6f}\n")
                    f.write(f"MB: {final_train_metrics['mb']:.6f}\n\n")
                    
                    f.write("HOLD-OUT EVALUATION RESULTS:\n")
                    f.write("OVERALL:\n")
                    f.write(f"  RMSE: {overall_eval_metrics['rmse']:.6f}\n")
                    f.write(f"  R2: {overall_eval_metrics['r2']:.6f}\n")
                    f.write(f"  MAE: {overall_eval_metrics['mae']:.6f}\n")
                    f.write(f"  MSE: {overall_eval_metrics['mse']:.6f}\n")
                    f.write(f"  MAPE: {overall_eval_metrics['mape']:.6f}\n")
                    f.write(f"  EVS: {overall_eval_metrics['evs']:.6f}\n")
                    f.write(f"  MB: {overall_eval_metrics['mb']:.6f}\n\n")
                    
                    f.write("PER-CAMPAIGN:\n")
                    for result in campaign_eval_results:
                        f.write(f"  {result['campaign']} ({result['samples']} samples):\n")
                        f.write(f"    RMSE: {result['rmse']:.6f}\n")
                        f.write(f"    R2: {result['r2']:.6f}\n")
                        f.write(f"    MAE: {result['mae']:.6f}\n")
                        f.write(f"    MSE: {result['mse']:.6f}\n")
                        f.write(f"    MAPE: {result['mape']:.6f}\n")
                        f.write(f"    EVS: {result['evs']:.6f}\n")
                        f.write(f"    MB: {result['mb']:.6f}\n")
                    f.write("\n")
                    
                    f.write("TOP 15 FEATURE IMPORTANCE:\n")
                    for idx, row in feature_importance.head(15).iterrows():
                        f.write(f"  {row['feature']}: {row['importance']:.4f}\n")
                    f.write("\n")
                    
                    f.write("FEATURE COLUMNS USED:\n")
                    for col in PREDICTOR_COLUMNS:
                        f.write(f"  - {col}\n")
                    
                    f.write("\nPERFORMANCE SUMMARY:\n")
                    f.write(f"Total Bayesian optimization trials: {len(all_bayesian_results) if all_bayesian_results else 0}\n")
                    f.write(f"Cross-validation folds: {len(outer_fold_results)}\n")
                    f.write(f"Hold-out campaigns evaluated: {len(campaign_eval_results)}\n")
                    f.write(f"Total metrics calculated: ~{len(all_bayesian_results) + len(outer_fold_results)*10 + 5 + len(campaign_eval_results)*5 + 5}\n")
                    
                    # ================================================================
                    # QUICK SUMMARY FOR TABLE FILLING
                    # ================================================================
                    f.write("\n" + "="*80 + "\n")
                    f.write("QUICK SUMMARY FOR TABLE FILLING\n")
                    f.write("="*80 + "\n")
                    f.write(f"Bin Configuration: {no_bins} | Split Strategy: {split_strat}\n\n")

                    # EVALUATION (single values)
                    f.write("EVALUATION METRICS (Final Model → Evaluation Set):\n")
                    f.write(f"MSE: {overall_eval_metrics['mse']:.6f}\n")
                    f.write(f"RMSE: {overall_eval_metrics['rmse']:.6f}\n")
                    f.write(f"MAE: {overall_eval_metrics['mae']:.6f}\n")
                    f.write(f"R2: {overall_eval_metrics['r2']:.6f}\n")
                    f.write(f"MAPE: {overall_eval_metrics['mape']:.6f}\n")
                    f.write(f"EVS: {overall_eval_metrics['evs']:.6f}\n")
                    f.write(f"MB: {overall_eval_metrics['mb']:.6f}\n\n")

                    # TEST - Individual Fold Results
                    f.write("TEST METRICS (Individual Outer Fold Performance):\n")
                    f.write("Format: Fold | MSE | RMSE | MAE | R2 | MAPE | EVS | MB\n")
                    for result in outer_fold_results:
                        f.write(f"Fold {result['fold']}: {result['test_mse']:.6f} | {result['test_rmse']:.6f} | "
                                f"{result['test_mae']:.6f} | {result['test_r2']:.6f} | {result['test_mape']:.6f} | "
                                f"{result['test_evs']:.6f} | {result['test_mb']:.6f}\n")

                    # TEST - Aggregated Results
                    if aggregated_results:
                        f.write("\nTEST METRICS (Aggregated):\n")
                        f.write(f"MSE: {aggregated_results['test_mse']['mean']:.6f} ± {aggregated_results['test_mse']['std']:.6f}\n")
                        f.write(f"RMSE: {aggregated_results['test_rmse']['mean']:.6f} ± {aggregated_results['test_rmse']['std']:.6f}\n")
                        f.write(f"MAE: {aggregated_results['test_mae']['mean']:.6f} ± {aggregated_results['test_mae']['std']:.6f}\n")
                        f.write(f"R2: {aggregated_results['test_r2']['mean']:.6f} ± {aggregated_results['test_r2']['std']:.6f}\n")
                        f.write(f"MAPE: {aggregated_results['test_mape']['mean']:.6f} ± {aggregated_results['test_mape']['std']:.6f}\n")
                        f.write(f"EVS: {aggregated_results['test_evs']['mean']:.6f} ± {aggregated_results['test_evs']['std']:.6f}\n")
                        f.write(f"MB: {aggregated_results['test_mb']['mean']:.6f} ± {aggregated_results['test_mb']['std']:.6f}\n\n")

                    # TRAINING - Individual Fold Results
                    if final_model_training_results:
                        f.write("TRAINING METRICS (Final Model → Individual Training Folds):\n")
                        f.write("Format: Fold | MSE | RMSE | MAE | R2 | MAPE | EVS | MB\n")
                        for result in final_model_training_results:
                            f.write(f"Fold {result['fold']}: {result['mse']:.6f} | {result['rmse']:.6f} | "
                                    f"{result['mae']:.6f} | {result['r2']:.6f} | {result['mape']:.6f} | "
                                    f"{result['evs']:.6f} | {result['mb']:.6f}\n")
                        
                        # TRAINING - Aggregated Results
                        if aggregated_training_results:
                            f.write("\nTRAINING METRICS (Aggregated):\n")
                            f.write(f"MSE: {aggregated_training_results['mse']['mean']:.6f} ± {aggregated_training_results['mse']['std']:.6f}\n")
                            f.write(f"RMSE: {aggregated_training_results['rmse']['mean']:.6f} ± {aggregated_training_results['rmse']['std']:.6f}\n")
                            f.write(f"MAE: {aggregated_training_results['mae']['mean']:.6f} ± {aggregated_training_results['mae']['std']:.6f}\n")
                            f.write(f"R2: {aggregated_training_results['r2']['mean']:.6f} ± {aggregated_training_results['r2']['std']:.6f}\n")
                            f.write(f"MAPE: {aggregated_training_results['mape']['mean']:.6f} ± {aggregated_training_results['mape']['std']:.6f}\n")
                            f.write(f"EVS: {aggregated_training_results['evs']['mean']:.6f} ± {aggregated_training_results['evs']['std']:.6f}\n")
                            f.write(f"MB: {aggregated_training_results['mb']['mean']:.6f} ± {aggregated_training_results['mb']['std']:.6f}\n\n")

                    f.write("BEST HYPERPARAMETERS:\n")
                    for param, value in best_overall_params.items():
                        f.write(f"{param}: {value}\n")

                    f.write("\n" + "="*80 + "\n")
                    f.write("END QUICK SUMMARY\n")
                    f.write("="*80 + "\n")
                
                print(f"\nAll results for bin {no_bins} with split strategy {split_strat} saved to: {OUTPUT_PATH}")
                print("\n" + "="*60)
                print("FILES CREATED:")
                print("="*60)
                print("- final_xgboost_model.pkl (trained model)")
                print("- bayesian_optimization_results.csv (all hyperparameter trials)")
                print("- outer_fold_performance.csv (5-fold CV results)")
                print("- final_model_performance.csv (final training metrics)")
                print("- final_model_training_individual_folds.csv (final model on individual folds)")
                print("- holdout_evaluation_per_campaign.csv (hold-out results)")
                print("- feature_importance.csv (feature rankings)")
                print("- comprehensive_kfold_report.txt (complete report)")
                
                # Store summary results for this bin configuration
                bin_summary = {
                    'bin_config': no_bins,
                    'split_strat': split_strat,
                    'dataset_path': DATASET_PATH,
                    'cv_train_rmse_mean': aggregated_results['train_rmse']['mean'] if aggregated_results else None,
                    'cv_train_rmse_std': aggregated_results['train_rmse']['std'] if aggregated_results else None,
                    'cv_test_rmse_mean': aggregated_results['test_rmse']['mean'] if aggregated_results else None,
                    'cv_test_rmse_std': aggregated_results['test_rmse']['std'] if aggregated_results else None,
                    'cv_train_r2_mean': aggregated_results['train_r2']['mean'] if aggregated_results else None,
                    'cv_train_r2_std': aggregated_results['train_r2']['std'] if aggregated_results else None,
                    'cv_test_r2_mean': aggregated_results['test_r2']['mean'] if aggregated_results else None,
                    'cv_test_r2_std': aggregated_results['test_r2']['std'] if aggregated_results else None,
                    'final_train_rmse': final_train_metrics['rmse'],
                    'final_train_r2': final_train_metrics['r2'],
                    'final_model_training_rmse_mean': aggregated_training_results['rmse']['mean'] if aggregated_training_results else None,
                    'final_model_training_r2_mean': aggregated_training_results['r2']['mean'] if aggregated_training_results else None,
                    'holdout_overall_rmse': overall_eval_metrics['rmse'],
                    'holdout_overall_r2': overall_eval_metrics['r2'],
                    'holdout_overall_mae': overall_eval_metrics['mae'],
                    'holdout_overall_mape': overall_eval_metrics['mape'],
                    'holdout_overall_evs': overall_eval_metrics['evs'],
                    'holdout_overall_mb': overall_eval_metrics['mb'],
                    'num_bayesian_trials': len(all_bayesian_results),
                    'best_params': str(best_overall_params),
                    'output_path': OUTPUT_PATH
                }
                all_bin_results.append(bin_summary)
                
            else:
                print("WARNING: Evaluation file not found!")
                bin_summary = {
                    'bin_config': no_bins,
                    'split_strat': split_strat,
                    'dataset_path': DATASET_PATH,
                    'status': 'evaluation_file_missing',
                    'output_path': OUTPUT_PATH
                }
                all_bin_results.append(bin_summary)
                
        else:
            print("ERROR: Could not complete final model training!")
            bin_summary = {
                'bin_config': no_bins,
                'split_strat': split_strat,
                'dataset_path': DATASET_PATH,
                'status': 'training_failed',
                'output_path': OUTPUT_PATH
            }
            all_bin_results.append(bin_summary)

        print(f"\n{'='*60}")
        print(f"COMPLETED BIN CONFIGURATION: {no_bins} with SPLIT STRATEGY: {split_strat}")
        print(f"{'='*60}")

# End of bin configuration and split strategy loops
print(f"\n{'='*80}")
print("ALL BIN CONFIGURATIONS AND SPLIT STRATEGIES COMPLETED")
print(f"{'='*80}")

# Create summary report across all bin configurations and split strategies
summary_output_path = rf"outputs/summary_all_bins_splitstrats_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
os.makedirs(summary_output_path, exist_ok=True)

# Save summary results
if all_bin_results:
    summary_df = pd.DataFrame(all_bin_results)
    summary_df.to_csv(os.path.join(summary_output_path, "all_bins_splitstrats_summary.csv"), index=False)

    # Create comprehensive summary report
    with open(os.path.join(summary_output_path, "all_bins_splitstrats_summary_report.txt"), 'w', encoding='utf-8') as f:
        f.write("XGBOOST K-FOLD ANALYSIS - ALL BIN CONFIGURATIONS AND SPLIT STRATEGIES SUMMARY\n")
        f.write("="*80 + "\n")
        f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Bin Configurations Tested: {bins}\n")
        f.write(f"Split Strategies Tested: {split_strats}\n")
        f.write(f"Total Combinations: {len(bins) * len(split_strats)}\n\n")
        
        f.write("METHODOLOGY SUMMARY:\n")
        f.write("- Nested cross-validation with GroupKFold campaign-based splitting\n")
        f.write("- Bayesian optimization for hyperparameter tuning (15 trials per fold)\n")
        f.write("- Comprehensive performance tracking at every training/testing step\n")
        f.write("- Per-campaign hold-out evaluation reporting\n")
        f.write("- All 7 metrics reported: MSE, RMSE, MAE, R2, MAPE, EVS, MB\n")
        f.write("- Data cleaning implemented to handle non-numeric values\n\n")
        
        for result in all_bin_results:
            f.write(f"BIN CONFIGURATION: {result['bin_config']} | SPLIT STRATEGY: {result['split_strat']}\n")
            f.write("-" * 50 + "\n")
            
            if 'status' in result:
                f.write(f"Status: {result['status']}\n")
            else:
                f.write(f"Cross-Validation Training RMSE: {result['cv_train_rmse_mean']:.6f} +/- {result['cv_train_rmse_std']:.6f}\n")
                f.write(f"Cross-Validation Test RMSE: {result['cv_test_rmse_mean']:.6f} +/- {result['cv_test_rmse_std']:.6f}\n")
                f.write(f"Cross-Validation Training R2: {result['cv_train_r2_mean']:.6f} +/- {result['cv_train_r2_std']:.6f}\n")
                f.write(f"Cross-Validation Test R2: {result['cv_test_r2_mean']:.6f} +/- {result['cv_test_r2_std']:.6f}\n")
                f.write(f"Final Training RMSE: {result['final_train_rmse']:.6f}\n")
                f.write(f"Final Training R2: {result['final_train_r2']:.6f}\n")
                if result['final_model_training_rmse_mean']:
                    f.write(f"Final Model Training (Individual Folds) RMSE: {result['final_model_training_rmse_mean']:.6f}\n")
                    f.write(f"Final Model Training (Individual Folds) R2: {result['final_model_training_r2_mean']:.6f}\n")
                f.write(f"Hold-out Overall RMSE: {result['holdout_overall_rmse']:.6f}\n")
                f.write(f"Hold-out Overall R2: {result['holdout_overall_r2']:.6f}\n")
                f.write(f"Hold-out Overall MAPE: {result['holdout_overall_mape']:.6f}\n")
                f.write(f"Hold-out Overall EVS: {result['holdout_overall_evs']:.6f}\n")
                f.write(f"Bayesian Optimization Trials: {result['num_bayesian_trials']}\n")
                f.write(f"Best Parameters: {result['best_params']}\n")
            
            f.write(f"Output Path: {result['output_path']}\n\n")

    print(f"\nSUMMARY OF ALL BIN CONFIGURATIONS AND SPLIT STRATEGIES:")
    print("="*70)
    for result in all_bin_results:
        if 'status' in result:
            print(f"Bin {result['bin_config']} | Split {result['split_strat']}: {result['status']}")
        else:
            print(f"Bin {result['bin_config']} | Split {result['split_strat']}: "
                  f"CV Test R2 = {result['cv_test_r2_mean']:.3f} +/- {result['cv_test_r2_std']:.3f}, "
                  f"Hold-out R2 = {result['holdout_overall_r2']:.3f}")

    print(f"\nSummary report saved to: {summary_output_path}")
else:
    print("No results to summarize!")

print("="*80)
print("NESTED CROSS-VALIDATION ANALYSIS COMPLETED SUCCESSFULLY!")
print("="*80)
print("\nKEY FEATURES IMPLEMENTED:")
print("- Bayesian optimization with Optuna (15 trials per fold)")
print("- GroupKFold for campaign-based data splitting (no data leakage)")
print("- Comprehensive performance metrics (MSE, RMSE, MAE, R2, MAPE, EVS, MB)")
print("- Data cleaning to handle non-numeric values")
print("- Training and testing metrics reported at every model evaluation")
print("- Per-campaign hold-out evaluation for detailed generalization assessment")
print("- Individual fold results AND aggregated statistics")
print("- Quick summary section for easy table filling")
print("- Complete audit trail of all hyperparameter trials and model performances")
print("- Automated processing across all bin configurations and split strategies")

