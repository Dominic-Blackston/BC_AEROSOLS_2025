import pandas as pd
import os
import sys
from datetime import datetime
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import RobustScaler
import numpy as np
import joblib

# Change to this script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)


#Determine bin numbers AND split strategies to test
bins = [3.1, 3, 4, 5]
split_strats = [1, 2]

for no_bins in bins:
    for split_strat in split_strats:
        print(f"Running xgboost_campaign.py with {no_bins} bins and split strategy {split_strat}")

        if no_bins == 3.1:
            DATASET_PATH = rf"split_datasets/data_split_strat{split_strat}/MAC_DATASET_LLOD_FILTERED/campaign_based_split"
            OUTPUT_PATH = rf"outputs/output_xgboost_campaign_{no_bins}bins_{split_strat}splitstrat_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        else:
            # SET YOUR DATASET PATH HERE
            DATASET_PATH = rf"split_datasets/data_split_strat{split_strat}/MAC_binning_{no_bins}bins_optimized/campaign_based_split"
            OUTPUT_PATH = rf"outputs/output_xgboost_campaign_{no_bins}bins_{split_strat}splitstrat_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Columns to be used for training and prediction
        PREDICTOR_COLUMNS = ['N1', 'N2', 'N3', 'N_total', 'V1', 'V2', 'V3', 'V_total', 'SAE', 'AAE', 'SSA_red', 'SSA_blue', 'SSA_green', 'b_scat_red', 'b_scat_blue', 'b_scat_green', 'b_abs_red', 'b_abs_blue', 'b_abs_green']
        PREDICTED_COLUMN = 'MAC_bc'

        # Create output directory
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        print(f"Output directory created: {OUTPUT_PATH}")

        print("="*70)
        print("XGBOOST CAMPAIGN-BASED SPLIT WITH ROBUST SCALER & OPTUNA OPTIMIZATION")
        print("="*70)
        print(f"Dataset path: {DATASET_PATH}")

        # Load the datasets
        print("Loading datasets...")
        train_path = os.path.join(DATASET_PATH, "train.csv")
        validation_path = os.path.join(DATASET_PATH, "validation.csv")
        evaluation_path = os.path.join(DATASET_PATH, "evaluation.csv")

        # Debug: Check which files exist
        print(f"Checking file paths:")
        print(f"Train path exists: {os.path.exists(train_path)} - {train_path}")
        print(f"Validation path exists: {os.path.exists(validation_path)} - {validation_path}")
        print(f"Evaluation path exists: {os.path.exists(evaluation_path)} - {evaluation_path}")

        if not all(os.path.exists(path) for path in [train_path, validation_path, evaluation_path]):
            print("Missing files detected!")
            if os.path.exists(DATASET_PATH):
                print(f"Files in directory: {os.listdir(DATASET_PATH)}")
            else:
                print("Base directory doesn't exist!")
            raise FileNotFoundError("One or more required CSV files not found in dataset path!")

        train_df = pd.read_csv(train_path)
        validation_df = pd.read_csv(validation_path)
        evaluation_df = pd.read_csv(evaluation_path)

        print(f"Train dataset shape: {train_df.shape}")
        print(f"Validation dataset shape: {validation_df.shape}")
        print(f"Evaluation dataset shape: {evaluation_df.shape}")

        # Check campaigns in each split
        if 'Campaign' in train_df.columns:
            train_campaigns = set(train_df['Campaign'].unique())
            validation_campaigns = set(validation_df['Campaign'].unique())
            eval_campaigns = set(evaluation_df['Campaign'].unique())
            
            print(f"\nCampaign distribution:")
            print(f"Train campaigns: {sorted(train_campaigns)}")
            print(f"Validation campaigns: {sorted(validation_campaigns)}")
            print(f"Evaluation campaigns: {sorted(eval_campaigns)}")
            
            # Check for overlap
            train_val_overlap = train_campaigns.intersection(validation_campaigns)
            if train_val_overlap:
                print(f"WARNING: Campaign overlap between train/validation: {sorted(train_val_overlap)}")

        # Prepare features and targets (BEFORE SCALING)
        X_train_raw = train_df[PREDICTOR_COLUMNS]
        y_train = train_df[PREDICTED_COLUMN]
        X_validation_raw = validation_df[PREDICTOR_COLUMNS]
        y_validation = validation_df[PREDICTED_COLUMN]
        X_evaluation_raw = evaluation_df[PREDICTOR_COLUMNS]
        y_evaluation = evaluation_df[PREDICTED_COLUMN]

        print(f"\nRaw feature shapes:")
        print(f"Training features: {X_train_raw.shape}")
        print(f"Validation features: {X_validation_raw.shape}")
        print(f"Evaluation features: {X_evaluation_raw.shape}")

        # ============================================================================
        # STEP 1: APPLY ROBUST SCALER NORMALIZATION
        # ============================================================================
        print("\n" + "="*60)
        print("STEP 1: APPLYING ROBUST SCALER NORMALIZATION")
        print("="*60)

        print("Initializing RobustScaler...")
        scaler = RobustScaler()

        print("Fitting RobustScaler on training data only...")
        X_train_scaled = scaler.fit_transform(X_train_raw)
        print(f"Training data scaled. Shape: {X_train_scaled.shape}")

        print("Applying fitted scaler to validation data...")
        X_validation_scaled = scaler.transform(X_validation_raw)
        print(f"Validation data scaled. Shape: {X_validation_scaled.shape}")

        print("Applying fitted scaler to evaluation data...")
        X_evaluation_scaled = scaler.transform(X_evaluation_raw)
        print(f"Evaluation data scaled. Shape: {X_evaluation_scaled.shape}")

        # Convert back to DataFrames to maintain column names and indices
        X_train = pd.DataFrame(X_train_scaled, columns=PREDICTOR_COLUMNS, index=X_train_raw.index)
        X_validation = pd.DataFrame(X_validation_scaled, columns=PREDICTOR_COLUMNS, index=X_validation_raw.index)
        X_evaluation = pd.DataFrame(X_evaluation_scaled, columns=PREDICTOR_COLUMNS, index=X_evaluation_raw.index)

        print("RobustScaler applied successfully!")

        # Print scaling statistics for verification
        print("\nScaling Statistics (Training Data):")
        print("Feature | Original Range | Scaled Range")
        print("-" * 50)
        for i, col in enumerate(PREDICTOR_COLUMNS):
            orig_min, orig_max = X_train_raw[col].min(), X_train_raw[col].max()
            scaled_min, scaled_max = X_train[col].min(), X_train[col].max()
            print(f"{col:12} | {orig_min:8.3f}-{orig_max:8.3f} | {scaled_min:6.3f}-{scaled_max:6.3f}")

        # Check for missing values in scaled data
        print(f"\nMissing values check (after scaling):")
        print(f"X_train missing: {X_train.isnull().sum().sum()}")
        print(f"y_train missing: {y_train.isnull().sum()}")
        print(f"X_validation missing: {X_validation.isnull().sum().sum()}")
        print(f"y_validation missing: {y_validation.isnull().sum()}")

        # Data summary (targets remain unchanged)
        print("\n" + "="*50)
        print("DATA SUMMARY (Targets - No Scaling Applied)")
        print("="*50)
        print(f"Train - MAC_bc range: {y_train.min():.6f} to {y_train.max():.6f}")
        print(f"Train - MAC_bc mean: {y_train.mean():.6f} ± {y_train.std():.6f}")
        print(f"Validation - MAC_bc range: {y_validation.min():.6f} to {y_validation.max():.6f}")
        print(f"Validation - MAC_bc mean: {y_validation.mean():.6f} ± {y_validation.std():.6f}")
        print(f"Evaluation - MAC_bc range: {y_evaluation.min():.6f} to {y_evaluation.max():.6f}")
        print(f"Evaluation - MAC_bc mean: {y_evaluation.mean():.6f} ± {y_evaluation.std():.6f}")

        # ============================================================================
        # STEP 2: OPTUNA BAYESIAN OPTIMIZATION HYPERPARAMETER TUNING
        # ============================================================================
        print("\n" + "="*60)
        print("STEP 2: OPTUNA BAYESIAN OPTIMIZATION HYPERPARAMETER TUNING")
        print("="*60)

        # Install and import Optuna
        try:
            import optuna
            print("Optuna loaded successfully")
        except ImportError:
            print("Installing Optuna...")
            import subprocess
            import sys
            subprocess.check_call([sys.executable, "-m", "pip", "install", "optuna"])
            import optuna
            print("Optuna installed and loaded")

        # Suppress Optuna's logging for cleaner output
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        # Global variables for optimization
        best_validation_model = None
        best_score_global = float('inf')
        optimization_results = []

        def objective(trial):
            """
            Objective function for Optuna optimization.
            Returns validation RMSE (to be minimized).
            """
            global best_validation_model, best_score_global
            
            # Define hyperparameters using Optuna's suggest methods
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 500),
                'max_depth': trial.suggest_int('max_depth', 3, 15),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                'reg_alpha': trial.suggest_float('reg_alpha', 0.01, 10.0),
                'reg_lambda': trial.suggest_float('reg_lambda', 0.01, 10.0),
            }
            
            # Create XGBoost model with current parameters
            model = XGBRegressor(
                **params,
                random_state=42,
                n_jobs=-1,
                tree_method='hist',
                early_stopping_rounds=50,
                eval_metric='rmse'
            )
            
            try:
                # Train model with early stopping
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_validation, y_validation)],
                    verbose=False
                )
                
                # Make predictions on validation set
                y_pred_val = model.predict(X_validation)
                
                # Calculate validation RMSE
                val_rmse = np.sqrt(mean_squared_error(y_validation, y_pred_val))
                val_r2 = r2_score(y_validation, y_pred_val)
                
                # Store results
                result = {
                    'trial_number': trial.number,
                    **params,
                    'validation_rmse': val_rmse,
                    'validation_r2': val_r2,
                    'n_estimators_used': model.best_iteration if hasattr(model, 'best_iteration') else params['n_estimators']
                }
                optimization_results.append(result)
                
                # Update best model if this is the best so far
                if val_rmse < best_score_global:
                    best_score_global = val_rmse
                    best_validation_model = model
                    print(f"  Trial {trial.number}: New best RMSE = {val_rmse:.6f} (R² = {val_r2:.6f})")
                
                return val_rmse  # Return value to minimize
                
            except Exception as e:
                print(f"  Trial {trial.number}: Error - {e}")
                return float('inf')  # Return high value for failed runs

        # Create Optuna study
        print("Creating Optuna study for hyperparameter optimization...")
        study = optuna.create_study(
            direction='minimize',           # Minimize validation RMSE
            sampler=optuna.samplers.TPESampler(seed=42)  # Tree-structured Parzen Estimator
        )

        print("Optuna Optimization Search Space:")
        print("  n_estimators: 50-500 (int)")
        print("  max_depth: 3-15 (int)")
        print("  learning_rate: 0.01-0.3 (float)")
        print("  subsample: 0.6-1.0 (float)")
        print("  colsample_bytree: 0.6-1.0 (float)")
        print("  reg_alpha: 0.01-10.0 (float)")
        print("  reg_lambda: 0.01-10.0 (float)")

        # Run optimization
        print(f"\nStarting Optuna optimization...")
        print(f"- Number of trials: 50 (adjust based on computational budget)")
        print(f"- Early stopping: 50 rounds per trial")
        print(f"- Sampler: TPE (Tree-structured Parzen Estimator)")

        start_time = datetime.now()

        # Perform optimization
        study.optimize(objective, n_trials=50)

        tuning_time = datetime.now() - start_time

        print(f"\nOptuna optimization completed in: {tuning_time}")
        print(f"Best validation RMSE found: {study.best_value:.6f}")

        # Extract best parameters
        best_params = study.best_params
        best_score = study.best_value

        print(f"\nBest parameters found:")
        for param, value in best_params.items():
            if param in ['n_estimators', 'max_depth']:
                print(f"  {param}: {value}")
            else:
                print(f"  {param}: {value:.6f}")

        print(f"\nOptimization summary:")
        print(f"  Total trials: {len(study.trials)}")
        print(f"  Best trial: {study.best_trial.number}")
        print(f"  Best score: {study.best_value:.6f}")

        # Retrain best model with optimal parameters
        print(f"\nRetraining best model with optimal parameters...")
        best_validation_model = XGBRegressor(
            **best_params,
            random_state=42,
            n_jobs=-1,
            tree_method='hist'
        )

        # Train final validation model
        best_validation_model.fit(X_train, y_train)

        print(f"Best validation model retrained successfully!")
        print(f"Final validation RMSE: {best_score:.6f}")

        # Save Optuna optimization results
        optuna_results_df = pd.DataFrame(optimization_results)
        print(f"\nOptuna optimization evaluated {len(optimization_results)} parameter combinations")

        # Optional: Create optimization plots (if available)
        try:
            # Save optimization history plot
            fig = optuna.visualization.plot_optimization_history(study)
            fig.write_html(os.path.join(OUTPUT_PATH, "optuna_optimization_history.html"))
            
            # Save parameter importance plot
            fig = optuna.visualization.plot_param_importances(study)
            fig.write_html(os.path.join(OUTPUT_PATH, "optuna_param_importance.html"))
            
            print("Optuna optimization plots saved as HTML files")
            
        except Exception as e:
            print(f"Could not create Optuna plots: {e}")

        # Save Optuna study for future analysis
        joblib.dump(study, os.path.join(OUTPUT_PATH, "optuna_study.pkl"))
        print("Optuna study object saved")

        # Additional metrics calculation
        def mean_absolute_percentage_error(y_true, y_pred):
            epsilon = 1e-8
            return np.mean(np.abs((y_true - y_pred) / (y_true + epsilon))) * 100

        def explained_variance_score(y_true, y_pred):
            return 1 - np.var(y_true - y_pred) / np.var(y_true)

        def mean_bias(y_true, y_pred):
            """Calculate Mean Bias (MB) - Added as per instructions"""
            return np.mean(y_pred - y_true)

        # VALIDATION PERFORMANCE EVALUATION (using best model trained only on train.csv)
        print("\n" + "="*50)
        print("VALIDATION PERFORMANCE RESULTS")
        print("="*50)

        print("Evaluating validation performance with model trained ONLY on train.csv...")
        y_pred_validation_true = best_validation_model.predict(X_validation)

        validation_mse = mean_squared_error(y_validation, y_pred_validation_true)
        validation_rmse = np.sqrt(validation_mse)
        validation_mae = mean_absolute_error(y_validation, y_pred_validation_true)
        validation_r2 = r2_score(y_validation, y_pred_validation_true)
        validation_mape = mean_absolute_percentage_error(y_validation, y_pred_validation_true)
        validation_evs = explained_variance_score(y_validation, y_pred_validation_true)
        validation_mb = mean_bias(y_validation, y_pred_validation_true)

        print("Validation Performance Results (Train campaigns → Validation campaigns):")
        print(f"MSE: {validation_mse:.6f}")
        print(f"RMSE: {validation_rmse:.6f}")
        print(f"MAE: {validation_mae:.6f}")
        print(f"R²: {validation_r2:.6f}")
        print(f"MAPE: {validation_mape:.2f}%")
        print(f"EVS: {validation_evs:.6f}")
        print(f"Mean Bias (MB): {validation_mb:.6f}")

        # Residual analysis for validation
        validation_residuals = y_validation - y_pred_validation_true
        validation_residual_mean = np.mean(validation_residuals)
        validation_residual_std = np.std(validation_residuals)

        print(f"Validation Residual Mean: {validation_residual_mean:.6f}")
        print(f"Validation Residual Std: {validation_residual_std:.6f}")

        # ============================================================================
        # STEP 3: TRAIN FINAL MODEL ON COMBINED DATA FOR EVALUATION
        # ============================================================================
        print("\n" + "="*50)
        print("STEP 3: TRAINING FINAL MODEL FOR EVALUATION")
        print("="*50)

        # Combine training and validation datasets (SCALED DATA)
        X_combined = pd.concat([X_train, X_validation], ignore_index=True)
        y_combined = pd.concat([y_train, y_validation], ignore_index=True)

        print(f"Combined training data shape: {X_combined.shape}")
        print(f"Combined features shape: {X_combined.shape}")

        # Train final model with best parameters
        final_model = XGBRegressor(
            **best_params,
            random_state=42,
            n_jobs=-1,
            tree_method='hist'
        )

        print("Training final model on combined scaled data...")
        final_training_start = datetime.now()

        final_model.fit(X_combined, y_combined)

        final_training_time = datetime.now() - final_training_start

        print(f"Final model training completed in: {final_training_time}")

        # ============================================================================
        # TRAINING PERFORMANCE EVALUATION ON COMBINED DATA
        # ============================================================================
        print("\n" + "="*50)
        print("TRAINING PERFORMANCE RESULTS")
        print("="*50)

        print("Evaluating training performance on combined training data...")
        y_pred_train = final_model.predict(X_combined)

        # Calculate training metrics
        train_mse = mean_squared_error(y_combined, y_pred_train)
        train_rmse = np.sqrt(train_mse)
        train_mae = mean_absolute_error(y_combined, y_pred_train)
        train_r2 = r2_score(y_combined, y_pred_train)
        train_mape = mean_absolute_percentage_error(y_combined, y_pred_train)
        train_evs = explained_variance_score(y_combined, y_pred_train)
        train_mb = mean_bias(y_combined, y_pred_train)

        print("Training Performance Results (Final model on combined training data):")
        print(f"MSE: {train_mse:.6f}")
        print(f"RMSE: {train_rmse:.6f}")
        print(f"MAE: {train_mae:.6f}")
        print(f"R²: {train_r2:.6f}")
        print(f"MAPE: {train_mape:.2f}%")
        print(f"EVS: {train_evs:.6f}")
        print(f"Mean Bias (MB): {train_mb:.6f}")

        # Training residual analysis
        train_residuals = y_combined - y_pred_train
        train_residual_mean = np.mean(train_residuals)
        train_residual_std = np.std(train_residuals)

        print(f"Training Residual Mean: {train_residual_mean:.6f}")
        print(f"Training Residual Std: {train_residual_std:.6f}")

        # ============================================================================
        # STEP 4: FINAL EVALUATION ON TEST CAMPAIGNS
        # ============================================================================
        print("\n" + "="*50)
        print("STEP 4: FINAL EVALUATION RESULTS")
        print("="*50)

        print("Making predictions on evaluation set...")
        y_pred_eval = final_model.predict(X_evaluation)

        # Calculate comprehensive metrics (including Mean Bias)
        mse = mean_squared_error(y_evaluation, y_pred_eval)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_evaluation, y_pred_eval)
        r2 = r2_score(y_evaluation, y_pred_eval)
        mape = mean_absolute_percentage_error(y_evaluation, y_pred_eval)
        evs = explained_variance_score(y_evaluation, y_pred_eval)
        mb = mean_bias(y_evaluation, y_pred_eval)

        # Residual analysis
        residuals = y_evaluation - y_pred_eval
        residual_mean = np.mean(residuals)
        residual_std = np.std(residuals)

        # Print results
        print("Final Evaluation Results (Combined model → DOE campaigns):")
        print(f"Mean Squared Error (MSE): {mse:.6f}")
        print(f"Root Mean Squared Error (RMSE): {rmse:.6f}")
        print(f"Mean Absolute Error (MAE): {mae:.6f}")
        print(f"Mean Absolute Percentage Error (MAPE): {mape:.2f}%")
        print(f"R² Score: {r2:.6f}")
        print(f"Explained Variance Score: {evs:.6f}")
        print(f"Mean Bias (MB): {mb:.6f}")
        print(f"Residual Mean: {residual_mean:.6f}")
        print(f"Residual Std: {residual_std:.6f}")

        # Feature importance
        print("\n" + "="*50)
        print("FEATURE IMPORTANCE")
        print("="*50)

        feature_importance = pd.DataFrame({
            'feature': PREDICTOR_COLUMNS,
            'importance': final_model.feature_importances_
        }).sort_values('importance', ascending=False)

        print("Top 15 most important features:")
        for idx, row in feature_importance.head(15).iterrows():
            print(f"  {row['feature']}: {row['importance']:.4f}")

        # ============================================================================
        # STEP 5: SAVE MODELS AND RESULTS
        # ============================================================================
        print(f"\nSaving results to: {OUTPUT_PATH}")

        # Save both models AND the scaler
        final_model_filename = os.path.join(OUTPUT_PATH, "final_xgboost_model_evaluation.pkl")
        validation_model_filename = os.path.join(OUTPUT_PATH, "best_validation_xgboost_model.pkl")
        scaler_filename = os.path.join(OUTPUT_PATH, "robust_scaler.pkl")

        joblib.dump(final_model, final_model_filename)
        joblib.dump(best_validation_model, validation_model_filename)
        joblib.dump(scaler, scaler_filename)

        print(f"Final model (for evaluation) saved to: {final_model_filename}")
        print(f"Best validation model saved to: {validation_model_filename}")
        print(f"RobustScaler saved to: {scaler_filename}")

        # Save feature importance
        feature_importance.to_csv(os.path.join(OUTPUT_PATH, "feature_importance.csv"), index=False)

        # Save predictions and residuals (evaluation set)
        evaluation_results_df = pd.DataFrame({
            'y_true': y_evaluation,
            'y_pred': y_pred_eval,
            'residuals': residuals
        })
        evaluation_results_df.to_csv(os.path.join(OUTPUT_PATH, "evaluation_predictions_and_residuals.csv"), index=False)

        # Save training predictions
        train_results_df = pd.DataFrame({
            'y_true': y_combined,
            'y_pred': y_pred_train,
            'residuals': train_residuals
        })
        train_results_df.to_csv(os.path.join(OUTPUT_PATH, "training_predictions_and_residuals.csv"), index=False)

        # Save validation predictions
        validation_results_df = pd.DataFrame({
            'y_true': y_validation,
            'y_pred': y_pred_validation_true,
            'residuals': validation_residuals
        })
        validation_results_df.to_csv(os.path.join(OUTPUT_PATH, "validation_predictions_and_residuals.csv"), index=False)

        # Save Optuna optimization results
        optuna_results_df.to_csv(os.path.join(OUTPUT_PATH, "optuna_optimization_results.csv"), index=False)

        # Save dataset info
        dataset_info = pd.DataFrame({
            'Dataset': ['Train', 'Validation', 'Evaluation', 'Combined_Final'],
            'Rows': [len(X_train), len(X_validation), len(X_evaluation), len(X_combined)],
            'Features': [len(PREDICTOR_COLUMNS)] * 4,
            'Scaling_Applied': ['RobustScaler'] * 4,
            'Description': [
                'Training campaigns (scaled)',
                'Validation campaigns (scaled)',
                'Evaluation campaigns (scaled)',
                'Combined train+validation for final model (scaled)'
            ]
        })
        dataset_info.to_csv(os.path.join(OUTPUT_PATH, "dataset_info.csv"), index=False)

        # Save comprehensive model report
        with open(os.path.join(OUTPUT_PATH, "campaign_model_report.txt"), 'w') as f:
            f.write("XGBOOST CAMPAIGN-BASED SPLIT MODEL REPORT\n")
            f.write("WITH ROBUST SCALER & OPTUNA OPTIMIZATION\n")
            f.write("="*70 + "\n")
            f.write(f"Training Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Dataset Path: {DATASET_PATH}\n")
            f.write(f"Output Path: {OUTPUT_PATH}\n")
            f.write(f"Preprocessing: RobustScaler applied to all features\n")
            f.write(f"Optimization: Optuna TPE with Early Stopping\n")
            f.write(f"Hyperparameter Tuning Time: {tuning_time}\n")
            f.write(f"Final Training Time: {final_training_time}\n\n")
            
            f.write("DATASET INFORMATION:\n")
            f.write(f"Training samples: {len(X_train):,} (scaled)\n")
            f.write(f"Validation samples: {len(X_validation):,} (scaled)\n")
            f.write(f"Evaluation samples: {len(X_evaluation):,} (scaled)\n")
            f.write(f"Combined training samples: {len(X_combined):,} (scaled)\n")
            f.write(f"Features: {len(PREDICTOR_COLUMNS)}\n\n")
            
            if 'Campaign' in train_df.columns:
                f.write("CAMPAIGN DISTRIBUTION:\n")
                f.write(f"Training campaigns: {sorted(train_campaigns)}\n")
                f.write(f"Validation campaigns: {sorted(validation_campaigns)}\n")
                f.write(f"Evaluation campaigns: {sorted(eval_campaigns)}\n\n")
            
            f.write("PREPROCESSING:\n")
            f.write("RobustScaler applied to all feature matrices\n")
            f.write("- Fitted on training data only\n")
            f.write("- Applied to validation and evaluation data\n")
            f.write("- Target variable (MAC_bc) kept in original units\n\n")
            
            f.write("OPTUNA OPTIMIZATION:\n")
            f.write(f"- Search space dimensions: 7 hyperparameters\n")
            f.write(f"- Total trials: {len(optimization_results)}\n")
            f.write(f"- Early stopping: 50 rounds per trial\n")
            f.write(f"- Sampler: TPE (Tree-structured Parzen Estimator)\n")
            f.write(f"- Best validation RMSE: {best_score:.6f}\n")
            f.write(f"- Best trial number: {study.best_trial.number}\n\n")
            
            f.write("BEST HYPERPARAMETERS:\n")
            for param, value in best_params.items():
                if param in ['n_estimators', 'max_depth']:
                    f.write(f"  {param}: {value}\n")
                else:
                    f.write(f"  {param}: {value:.6f}\n")
            f.write("\n")
            
            f.write("TRAINING PERFORMANCE (Final model on combined training data):\n")
            f.write(f"MSE: {train_mse:.6f}\n")
            f.write(f"RMSE: {train_rmse:.6f}\n")
            f.write(f"MAE: {train_mae:.6f}\n")
            f.write(f"R2: {train_r2:.6f}\n")
            f.write(f"MAPE: {train_mape:.2f}%\n")
            f.write(f"EVS: {train_evs:.6f}\n")
            f.write(f"Mean Bias (MB): {train_mb:.6f}\n")
            f.write(f"Residual Mean: {train_residual_mean:.6f}\n")
            f.write(f"Residual Std: {train_residual_std:.6f}\n\n")
            
            f.write("VALIDATION PERFORMANCE (Train-only model to Validation campaigns):\n")
            f.write(f"MSE: {validation_mse:.6f}\n")
            f.write(f"RMSE: {validation_rmse:.6f}\n")
            f.write(f"MAE: {validation_mae:.6f}\n")
            f.write(f"R2: {validation_r2:.6f}\n")
            f.write(f"MAPE: {validation_mape:.2f}%\n")
            f.write(f"EVS: {validation_evs:.6f}\n")
            f.write(f"Mean Bias (MB): {validation_mb:.6f}\n")
            f.write(f"Residual Mean: {validation_residual_mean:.6f}\n")
            f.write(f"Residual Std: {validation_residual_std:.6f}\n\n")
            
            f.write("FINAL EVALUATION PERFORMANCE (Combined model to DOE campaigns):\n")
            f.write(f"Mean Squared Error (MSE): {mse:.6f}\n")
            f.write(f"Root Mean Squared Error (RMSE): {rmse:.6f}\n")
            f.write(f"Mean Absolute Error (MAE): {mae:.6f}\n")
            f.write(f"Mean Absolute Percentage Error (MAPE): {mape:.2f}%\n")
            f.write(f"R2 Score: {r2:.6f}\n")
            f.write(f"Explained Variance Score: {evs:.6f}\n")
            f.write(f"Mean Bias (MB): {mb:.6f}\n")
            f.write(f"Residual Mean: {residual_mean:.6f}\n")
            f.write(f"Residual Std: {residual_std:.6f}\n\n")
            
            f.write("FEATURE IMPORTANCE (Top 15):\n")
            for idx, row in feature_importance.head(15).iterrows():
                f.write(f"  {row['feature']}: {row['importance']:.4f}\n")
            f.write("\n")
            
            f.write("FEATURE COLUMNS USED:\n")
            for col in PREDICTOR_COLUMNS:
                f.write(f"  - {col}\n")

        print("\n" + "="*60)
        print("FILES CREATED:")
        print("="*60)
        print("- final_xgboost_model_evaluation.pkl (model for final evaluation)")
        print("- best_validation_xgboost_model.pkl (model for validation performance)")
        print("- robust_scaler.pkl (scaler for future predictions)")
        print("- optuna_study.pkl (Optuna study object for analysis)")
        print("- feature_importance.csv (feature rankings)")
        print("- training_predictions_and_residuals.csv (training performance results)")
        print("- validation_predictions_and_residuals.csv (validation performance results)")
        print("- evaluation_predictions_and_residuals.csv (final evaluation results)")
        print("- optuna_optimization_results.csv (all tested parameters and results)")
        print("- optuna_optimization_history.html (interactive convergence plot)")
        print("- optuna_param_importance.html (interactive parameter importance plot)")
        print("- dataset_info.csv (dataset summary)")
        print("- campaign_model_report.txt (comprehensive report)")

        print("\n" + "="*70)
        print("CAMPAIGN-BASED TRAINING WITH ROBUST SCALER & OPTUNA OPTIMIZATION COMPLETED!")
        print("="*70)
        print("\nSUMMARY:")
        print(f"- Preprocessing: RobustScaler applied to all features")
        print(f"- Optimization: Optuna TPE with {len(optimization_results)} trials completed")
        print(f"- Early stopping: Implemented to prevent overfitting")
        print(f"- Training performance (combined model on training data): RMSE = {train_rmse:.6f}, R² = {train_r2:.6f}")
        print(f"- Validation performance (train campaigns to validation campaigns): RMSE = {validation_rmse:.6f}, R² = {validation_r2:.6f}")
        print(f"- Final evaluation (combined model to DOE campaigns): RMSE = {rmse:.6f}, R² = {r2:.6f}")
        print(f"- Best parameters found via Optuna optimization saved in report")
        print(f"- Interactive HTML plots created for optimization analysis")
        print("="*70)

        print("\nTo run this code:")
        print("1. Install required packages: pip install optuna xgboost scikit-learn pandas numpy joblib")
        print("2. Ensure your dataset files are in the correct directory structure")
        print("3. Run the script and check the outputs/ folder for results")
        print("\nOptuna will be automatically installed if not present.")
        print("All results, models, and interactive plots will be saved to the timestamped output directory.")