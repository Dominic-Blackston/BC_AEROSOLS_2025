import pandas as pd
import os
import sys
from datetime import datetime
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.model_selection import cross_val_score, StratifiedKFold, KFold
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
        print(f"Running xgboost_random.py with {no_bins} bins and split strategy {split_strat}")
        if no_bins == 3.1:
            DATASET_PATH = rf"split_datasets/data_split_strat{split_strat}/MAC_DATASET_LLOD_FILTERED/random_based_split"
            OUTPUT_PATH = rf"outputs/output_xgboost_random_{no_bins}bins_{split_strat}splitstrat_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        else:
            # SET YOUR DATASET PATH HERE
            DATASET_PATH = rf"split_datasets/data_split_strat{split_strat}/MAC_binning_{no_bins}bins_optimized/random_based_split"
            OUTPUT_PATH = rf"outputs/output_xgboost_random_{no_bins}bins_{split_strat}splitstrat_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Columns to be used for training and prediction
        PREDICTOR_COLUMNS = ['N1', 'N2', 'N3', 'N_total', 'V1', 'V2', 'V3', 'V_total', 'SAE', 'AAE', 'SSA_red', 'SSA_blue', 'SSA_green', 'b_scat_red', 'b_scat_blue', 'b_scat_green', 'b_abs_red', 'b_abs_blue', 'b_abs_green']
        PREDICTED_COLUMN = 'MAC_bc'

        # Create output directory
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        print(f"Output directory created: {OUTPUT_PATH}")

        print("="*70)
        print("XGBOOST RANDOM-BASED SPLIT WITH COMPLETE PIPELINE")
        print("="*70)
        print(f"Dataset path: {DATASET_PATH}")

        # Load the datasets (Step 1 - assumed correct)
        print("Loading datasets...")
        train_path = os.path.join(DATASET_PATH, "train.csv")
        test_path = os.path.join(DATASET_PATH, "test.csv")  # Internal test set (20% days)
        evaluation_path = os.path.join(DATASET_PATH, "evaluation.csv")  # Hold-out campaigns

        if not all(os.path.exists(path) for path in [train_path, test_path, evaluation_path]):
            raise FileNotFoundError("One or more required CSV files not found in dataset path!")

        train_df = pd.read_csv(train_path)
        test_df = pd.read_csv(test_path)  # Internal test set (20% days from same campaigns)
        evaluation_df = pd.read_csv(evaluation_path)  # Hold-out campaigns

        print(f"Train dataset shape: {train_df.shape}")
        print(f"Internal Test dataset shape: {test_df.shape}")
        print(f"Evaluation dataset shape: {evaluation_df.shape}")

        # Check campaigns in each split
        if 'Campaign' in train_df.columns:
            train_campaigns = set(train_df['Campaign'].unique())
            test_campaigns = set(test_df['Campaign'].unique())
            eval_campaigns = set(evaluation_df['Campaign'].unique())
            
            print(f"\nCampaign distribution:")
            print(f"Train campaigns: {sorted(train_campaigns)}")
            print(f"Internal Test campaigns: {sorted(test_campaigns)}")
            print(f"Evaluation campaigns: {sorted(eval_campaigns)}")
            
            # Check for overlap (expected for day-based)
            train_test_overlap = train_campaigns.intersection(test_campaigns)
            if train_test_overlap:
                print(f"Campaign overlap between train/test (expected for day-based): {sorted(train_test_overlap)}")

        # Prepare features and targets (BEFORE SCALING)
        X_train_raw = train_df[PREDICTOR_COLUMNS]
        y_train = train_df[PREDICTED_COLUMN]
        X_test_raw = test_df[PREDICTOR_COLUMNS]
        y_test = test_df[PREDICTED_COLUMN]
        X_evaluation_raw = evaluation_df[PREDICTOR_COLUMNS]
        y_evaluation = evaluation_df[PREDICTED_COLUMN]

        print(f"\nRaw feature shapes:")
        print(f"Training features: {X_train_raw.shape}")
        print(f"Internal Test features: {X_test_raw.shape}")
        print(f"Evaluation features: {X_evaluation_raw.shape}")

        # ============================================================================
        # STEP 2: DATA PREPROCESSING - ROBUST SCALER
        # ============================================================================
        print("\n" + "="*60)
        print("STEP 2: DATA PREPROCESSING - ROBUST SCALER")
        print("="*60)

        print("Initializing RobustScaler...")
        scaler = RobustScaler()

        print("Fitting RobustScaler on training data only...")
        X_train_scaled = scaler.fit_transform(X_train_raw)
        print(f"Training data scaled. Shape: {X_train_scaled.shape}")

        print("Applying fitted scaler to internal test data...")
        X_test_scaled = scaler.transform(X_test_raw)
        print(f"Internal test data scaled. Shape: {X_test_scaled.shape}")

        print("Applying fitted scaler to evaluation data...")
        X_evaluation_scaled = scaler.transform(X_evaluation_raw)
        print(f"Evaluation data scaled. Shape: {X_evaluation_scaled.shape}")

        # Convert back to DataFrames to maintain column names and indices
        X_train = pd.DataFrame(X_train_scaled, columns=PREDICTOR_COLUMNS, index=X_train_raw.index)
        X_test = pd.DataFrame(X_test_scaled, columns=PREDICTOR_COLUMNS, index=X_test_raw.index)
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
        print(f"X_test missing: {X_test.isnull().sum().sum()}")
        print(f"y_test missing: {y_test.isnull().sum()}")

        # Data summary (targets remain unchanged)
        print("\n" + "="*50)
        print("DATA SUMMARY (Targets - No Scaling Applied)")
        print("="*50)
        print(f"Train - MAC_bc range: {y_train.min():.6f} to {y_train.max():.6f}")
        print(f"Train - MAC_bc mean: {y_train.mean():.6f} ± {y_train.std():.6f}")
        print(f"Internal Test - MAC_bc range: {y_test.min():.6f} to {y_test.max():.6f}")
        print(f"Internal Test - MAC_bc mean: {y_test.mean():.6f} ± {y_test.std():.6f}")
        print(f"Evaluation - MAC_bc range: {y_evaluation.min():.6f} to {y_evaluation.max():.6f}")
        print(f"Evaluation - MAC_bc mean: {y_evaluation.mean():.6f} ± {y_evaluation.std():.6f}")

        # ============================================================================
        # STEP 3: HYPERPARAMETER TUNING WITH 10-FOLD CROSS-VALIDATION
        # ============================================================================
        print("\n" + "="*60)
        print("STEP 3: HYPERPARAMETER TUNING WITH 10-FOLD CROSS-VALIDATION")
        print("="*60)

        # Install and import optimization library
        try:
            import optuna
            print("Optuna loaded successfully for Bayesian optimization")
            use_bayesian = True
        except ImportError:
            print("Optuna not available, using Grid Search")
            use_bayesian = False

        # Define hyperparameter search space
        if use_bayesian:
            print("Using Bayesian Optimization with Optuna")
            
            # Suppress Optuna's logging for cleaner output
            optuna.logging.set_verbosity(optuna.logging.WARNING)
            
            cv_results = []
            best_cv_score = float('inf')
            best_params = None
            
            def cv_objective(trial):
                """Objective function for Optuna with cross-validation"""
                global best_cv_score, best_params, cv_results
                
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
                
                # Create model
                model = XGBRegressor(
                    **params,
                    random_state=42,
                    n_jobs=-1,
                    tree_method='hist'
                )
                
                # Perform 10-fold cross-validation
                kfold = KFold(n_splits=10, shuffle=True, random_state=42)
                cv_scores = cross_val_score(model, X_train, y_train, 
                                        cv=kfold, scoring='neg_mean_squared_error', n_jobs=-1)
                
                # Convert to RMSE and take average
                cv_rmse_scores = np.sqrt(-cv_scores)
                mean_cv_rmse = np.mean(cv_rmse_scores)
                std_cv_rmse = np.std(cv_rmse_scores)
                
                # Store results
                result = {
                    'trial_number': trial.number,
                    **params,
                    'cv_mean_rmse': mean_cv_rmse,
                    'cv_std_rmse': std_cv_rmse,
                    'cv_scores': cv_rmse_scores.tolist()
                }
                cv_results.append(result)
                
                # Update best parameters
                if mean_cv_rmse < best_cv_score:
                    best_cv_score = mean_cv_rmse
                    best_params = params.copy()
                    print(f"  Trial {trial.number}: New best CV RMSE = {mean_cv_rmse:.6f} ± {std_cv_rmse:.6f}")
                
                return mean_cv_rmse
            
            # Create study and optimize
            study = optuna.create_study(direction='minimize', sampler=optuna.samplers.TPESampler(seed=42))
            
            print("Starting Bayesian optimization with 10-fold CV...")
            print("- Number of trials: 10")
            print("- Cross-validation: 10-fold")
            print("- Metric: RMSE")
            
            start_time = datetime.now()
            study.optimize(cv_objective, n_trials=10)
            tuning_time = datetime.now() - start_time
            
            best_params = study.best_params
            best_cv_score = study.best_value
            
        else:
            # Grid Search with Cross-Validation
            from itertools import product
            
            print("Using Grid Search with 10-fold Cross-Validation")
            
            param_grid = {
                'n_estimators': [100, 200, 300],
                'max_depth': [6, 8, 10],
                'learning_rate': [0.01, 0.05, 0.1],
                'subsample': [0.8, 0.9],
                'colsample_bytree': [0.8, 0.9]
            }
            
            param_combinations = list(product(
                param_grid['n_estimators'],
                param_grid['max_depth'],
                param_grid['learning_rate'],
                param_grid['subsample'],
                param_grid['colsample_bytree']
            ))
            
            print(f"Testing {len(param_combinations)} parameter combinations with 10-fold CV...")
            
            cv_results = []
            best_cv_score = float('inf')
            best_params = None
            
            start_time = datetime.now()
            
            for i, (n_est, max_d, lr, subsamp, colsamp) in enumerate(param_combinations):
                if i % 5 == 0:
                    print(f"  Progress: {i+1}/{len(param_combinations)}")
                
                params = {
                    'n_estimators': n_est,
                    'max_depth': max_d,
                    'learning_rate': lr,
                    'subsample': subsamp,
                    'colsample_bytree': colsamp
                }
                
                # Create model
                model = XGBRegressor(
                    **params,
                    random_state=42,
                    n_jobs=-1,
                    tree_method='hist'
                )
                
                # Perform 10-fold cross-validation
                kfold = KFold(n_splits=10, shuffle=True, random_state=42)
                cv_scores = cross_val_score(model, X_train, y_train, 
                                        cv=kfold, scoring='neg_mean_squared_error', n_jobs=-1)
                
                # Convert to RMSE and take average
                cv_rmse_scores = np.sqrt(-cv_scores)
                mean_cv_rmse = np.mean(cv_rmse_scores)
                std_cv_rmse = np.std(cv_rmse_scores)
                
                # Store results
                result = {
                    'combination_number': i,
                    **params,
                    'cv_mean_rmse': mean_cv_rmse,
                    'cv_std_rmse': std_cv_rmse,
                    'cv_scores': cv_rmse_scores.tolist()
                }
                cv_results.append(result)
                
                # Update best parameters
                if mean_cv_rmse < best_cv_score:
                    best_cv_score = mean_cv_rmse
                    best_params = params.copy()
            
            tuning_time = datetime.now() - start_time

        print(f"\nHyperparameter tuning completed in: {tuning_time}")
        print(f"Best cross-validation RMSE: {best_cv_score:.6f}")
        print(f"Best parameters found:")
        for param, value in best_params.items():
            if param in ['n_estimators', 'max_depth']:
                print(f"  {param}: {value}")
            else:
                print(f"  {param}: {value:.6f}")

        # Save CV results
        cv_results_df = pd.DataFrame(cv_results)
        print(f"\nCross-validation evaluated {len(cv_results)} parameter combinations")

        # ============================================================================
        # STEP 4: FINAL MODEL TRAINING
        # ============================================================================
        print("\n" + "="*50)
        print("STEP 4: FINAL MODEL TRAINING")
        print("="*50)

        print("Training final model on entire training set with best hyperparameters...")
        final_model = XGBRegressor(
            **best_params,
            random_state=42,
            n_jobs=-1,
            tree_method='hist'
        )

        final_training_start = datetime.now()
        final_model.fit(X_train, y_train)
        final_training_time = datetime.now() - final_training_start

        print(f"Final model training completed in: {final_training_time}")

        # Additional metrics calculation
        def mean_absolute_percentage_error(y_true, y_pred):
            epsilon = 1e-8
            return np.mean(np.abs((y_true - y_pred) / (y_true + epsilon))) * 100

        def explained_variance_score(y_true, y_pred):
            return 1 - np.var(y_true - y_pred) / np.var(y_true)

        def mean_bias(y_true, y_pred):
            """Calculate Mean Bias (MB)"""
            return np.mean(y_pred - y_true)

        # ============================================================================
        # STEP 5: EVALUATION ON TRAINING AND INTERNAL TEST SET
        # ============================================================================
        print("\n" + "="*50)
        print("STEP 5: TRAINING AND INTERNAL TEST SET EVALUATION")
        print("="*50)

        # Training Set Performance
        print("Evaluating performance on training set...")
        y_pred_train = final_model.predict(X_train)

        train_mse = mean_squared_error(y_train, y_pred_train)
        train_rmse = np.sqrt(train_mse)
        train_mae = mean_absolute_error(y_train, y_pred_train)
        train_r2 = r2_score(y_train, y_pred_train)
        train_mape = mean_absolute_percentage_error(y_train, y_pred_train)
        train_evs = explained_variance_score(y_train, y_pred_train)
        train_mb = mean_bias(y_train, y_pred_train)

        print("Training Set Performance Results:")
        print(f"MSE: {train_mse:.6f}")
        print(f"RMSE: {train_rmse:.6f}")
        print(f"MAE: {train_mae:.6f}")
        print(f"R²: {train_r2:.6f}")
        print(f"MAPE: {train_mape:.2f}%")
        print(f"EVS: {train_evs:.6f}")
        print(f"Mean Bias (MB): {train_mb:.6f}")

        # Training residual analysis
        train_residuals = y_train - y_pred_train
        train_residual_mean = np.mean(train_residuals)
        train_residual_std = np.std(train_residuals)

        print(f"Training Residual Mean: {train_residual_mean:.6f}")
        print(f"Training Residual Std: {train_residual_std:.6f}")

        # Internal Test Set Performance
        print("\nEvaluating performance on internal test set...")
        y_pred_test = final_model.predict(X_test)

        test_mse = mean_squared_error(y_test, y_pred_test)
        test_rmse = np.sqrt(test_mse)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        test_r2 = r2_score(y_test, y_pred_test)
        test_mape = mean_absolute_percentage_error(y_test, y_pred_test)
        test_evs = explained_variance_score(y_test, y_pred_test)
        test_mb = mean_bias(y_test, y_pred_test)

        print("Internal Test Set Performance Results (Temporal Generalization):")
        print(f"MSE: {test_mse:.6f}")
        print(f"RMSE: {test_rmse:.6f}")
        print(f"MAE: {test_mae:.6f}")
        print(f"R²: {test_r2:.6f}")
        print(f"MAPE: {test_mape:.2f}%")
        print(f"EVS: {test_evs:.6f}")
        print(f"Mean Bias (MB): {test_mb:.6f}")

        # Test residual analysis
        test_residuals = y_test - y_pred_test
        test_residual_mean = np.mean(test_residuals)
        test_residual_std = np.std(test_residuals)

        print(f"Internal Test Residual Mean: {test_residual_mean:.6f}")
        print(f"Internal Test Residual Std: {test_residual_std:.6f}")

        # ============================================================================
        # STEP 6: FINAL EVALUATION ON HELD-OUT CAMPAIGNS
        # ============================================================================
        print("\n" + "="*50)
        print("STEP 6: FINAL EVALUATION ON HELD-OUT CAMPAIGNS")
        print("="*50)

        print("Making predictions on held-out evaluation campaigns...")
        y_pred_eval = final_model.predict(X_evaluation)

        # Overall evaluation performance
        eval_mse = mean_squared_error(y_evaluation, y_pred_eval)
        eval_rmse = np.sqrt(eval_mse)
        eval_mae = mean_absolute_error(y_evaluation, y_pred_eval)
        eval_r2 = r2_score(y_evaluation, y_pred_eval)
        eval_mape = mean_absolute_percentage_error(y_evaluation, y_pred_eval)
        eval_evs = explained_variance_score(y_evaluation, y_pred_eval)
        eval_mb = mean_bias(y_evaluation, y_pred_eval)

        print("Overall Evaluation Set Performance Results (Campaign Generalization):")
        print(f"MSE: {eval_mse:.6f}")
        print(f"RMSE: {eval_rmse:.6f}")
        print(f"MAE: {eval_mae:.6f}")
        print(f"R²: {eval_r2:.6f}")
        print(f"MAPE: {eval_mape:.2f}%")
        print(f"EVS: {eval_evs:.6f}")
        print(f"Mean Bias (MB): {eval_mb:.6f}")

        # Evaluation residual analysis
        eval_residuals = y_evaluation - y_pred_eval
        eval_residual_mean = np.mean(eval_residuals)
        eval_residual_std = np.std(eval_residuals)

        print(f"Evaluation Residual Mean: {eval_residual_mean:.6f}")
        print(f"Evaluation Residual Std: {eval_residual_std:.6f}")

        # Separate evaluation by individual campaigns
        print("\nIndividual Campaign Performance:")
        campaign_results = {}
        for campaign in eval_campaigns:
            campaign_mask = evaluation_df['Campaign'] == campaign
            if campaign_mask.sum() > 0:
                y_true_camp = y_evaluation[campaign_mask]
                y_pred_camp = y_pred_eval[campaign_mask]
                
                camp_mse = mean_squared_error(y_true_camp, y_pred_camp)
                camp_rmse = np.sqrt(camp_mse)
                camp_mae = mean_absolute_error(y_true_camp, y_pred_camp)
                camp_r2 = r2_score(y_true_camp, y_pred_camp)
                camp_mape = mean_absolute_percentage_error(y_true_camp, y_pred_camp)
                camp_evs = explained_variance_score(y_true_camp, y_pred_camp)
                camp_mb = mean_bias(y_true_camp, y_pred_camp)
                
                campaign_results[campaign] = {
                    'samples': len(y_true_camp),
                    'mse': camp_mse,
                    'rmse': camp_rmse,
                    'mae': camp_mae,
                    'r2': camp_r2,
                    'mape': camp_mape,
                    'evs': camp_evs,
                    'mb': camp_mb
                }
                
                print(f"\n{campaign} ({len(y_true_camp)} samples):")
                print(f"  MSE: {camp_mse:.6f}")
                print(f"  RMSE: {camp_rmse:.6f}")
                print(f"  MAE: {camp_mae:.6f}")
                print(f"  R²: {camp_r2:.6f}")
                print(f"  MAPE: {camp_mape:.2f}%")
                print(f"  EVS: {camp_evs:.6f}")
                print(f"  Mean Bias (MB): {camp_mb:.6f}")

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
        # STEP 7: SAVE MODELS AND RESULTS
        # ============================================================================
        print(f"\nSaving results to: {OUTPUT_PATH}")

        # Save model and scaler
        final_model_filename = os.path.join(OUTPUT_PATH, "final_xgboost_day_model.pkl")
        scaler_filename = os.path.join(OUTPUT_PATH, "robust_scaler.pkl")

        joblib.dump(final_model, final_model_filename)
        joblib.dump(scaler, scaler_filename)

        print(f"Final model saved to: {final_model_filename}")
        print(f"RobustScaler saved to: {scaler_filename}")

        # Save feature importance
        feature_importance.to_csv(os.path.join(OUTPUT_PATH, "feature_importance.csv"), index=False)

        # Save predictions and residuals
        # Training set
        train_results_df = pd.DataFrame({
            'y_true': y_train,
            'y_pred': y_pred_train,
            'residuals': train_residuals
        })
        train_results_df.to_csv(os.path.join(OUTPUT_PATH, "training_set_results.csv"), index=False)

        # Internal test set
        test_results_df = pd.DataFrame({
            'y_true': y_test,
            'y_pred': y_pred_test,
            'residuals': test_residuals
        })
        test_results_df.to_csv(os.path.join(OUTPUT_PATH, "internal_test_set_results.csv"), index=False)

        # Evaluation set
        eval_results_df = pd.DataFrame({
            'y_true': y_evaluation,
            'y_pred': y_pred_eval,
            'residuals': eval_residuals,
            'campaign': evaluation_df['Campaign'].values
        })
        eval_results_df.to_csv(os.path.join(OUTPUT_PATH, "evaluation_set_results.csv"), index=False)

        # Save cross-validation results
        cv_results_df.to_csv(os.path.join(OUTPUT_PATH, "cross_validation_results.csv"), index=False)

        # Save individual campaign results
        campaign_results_df = pd.DataFrame.from_dict(campaign_results, orient='index')
        campaign_results_df.to_csv(os.path.join(OUTPUT_PATH, "individual_campaign_results.csv"))

        # Save dataset info
        dataset_info = pd.DataFrame({
            'Dataset': ['Training', 'Internal_Test', 'Evaluation'],
            'Rows': [len(X_train), len(X_test), len(X_evaluation)],
            'Features': [len(PREDICTOR_COLUMNS)] * 3,
            'Scaling_Applied': ['RobustScaler'] * 3,
            'Description': [
                'Training set (80% days from campaigns, scaled)',
                'Internal test set (20% days from same campaigns, scaled)',
                'Evaluation set (held-out campaigns, scaled)'
            ]
        })
        dataset_info.to_csv(os.path.join(OUTPUT_PATH, "dataset_info.csv"), index=False)

        # Save comprehensive model report
        with open(os.path.join(OUTPUT_PATH, "random_based_complete_report.txt"), 'w') as f:
            f.write("XGBOOST RANDOM-BASED SPLIT COMPLETE PIPELINE REPORT\n")
            f.write("="*70 + "\n")
            f.write(f"Training Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Dataset Path: {DATASET_PATH}\n")
            f.write(f"Output Path: {OUTPUT_PATH}\n")
            f.write(f"Preprocessing: RobustScaler applied to all features\n")
            f.write(f"Hyperparameter Method: {'Bayesian Optimization (Optuna)' if use_bayesian else 'Grid Search'}\n")
            f.write(f"Cross-Validation: 10-fold\n")
            f.write(f"Hyperparameter Tuning Time: {tuning_time}\n")
            f.write(f"Final Training Time: {final_training_time}\n\n")
            
            f.write("DATASET INFORMATION:\n")
            f.write(f"Training samples (80% days): {len(X_train):,} (scaled)\n")
            f.write(f"Internal test samples (20% days): {len(X_test):,} (scaled)\n")
            f.write(f"Evaluation samples (held-out campaigns): {len(X_evaluation):,} (scaled)\n")
            f.write(f"Features: {len(PREDICTOR_COLUMNS)}\n\n")
            
            if 'Campaign' in train_df.columns:
                f.write("CAMPAIGN DISTRIBUTION:\n")
                f.write(f"Training/Test campaigns: {sorted(train_campaigns)}\n")
                f.write(f"Evaluation campaigns: {sorted(eval_campaigns)}\n\n")
            
            f.write("PREPROCESSING:\n")
            f.write("RobustScaler applied to all feature matrices\n")
            f.write("- Fitted on training data only\n")
            f.write("- Applied to internal test and evaluation data\n")
            f.write("- Target variable (MAC_bc) kept in original units\n\n")
            
            f.write("CROSS-VALIDATION RESULTS:\n")
            f.write(f"- Method: 10-fold cross-validation\n")
            f.write(f"- Best CV RMSE: {best_cv_score:.6f}\n")
            f.write(f"- Total combinations tested: {len(cv_results)}\n\n")
            
            f.write("BEST HYPERPARAMETERS:\n")
            for param, value in best_params.items():
                if param in ['n_estimators', 'max_depth']:
                    f.write(f"  {param}: {value}\n")
                else:
                    f.write(f"  {param}: {value:.6f}\n")
            f.write("\n")
            
            f.write("TRAINING SET PERFORMANCE:\n")
            f.write(f"MSE: {train_mse:.6f}\n")
            f.write(f"RMSE: {train_rmse:.6f}\n")
            f.write(f"MAE: {train_mae:.6f}\n")
            f.write(f"R2: {train_r2:.6f}\n")
            f.write(f"MAPE: {train_mape:.2f}%\n")
            f.write(f"EVS: {train_evs:.6f}\n")
            f.write(f"Mean Bias (MB): {train_mb:.6f}\n")
            f.write(f"Residual Mean: {train_residual_mean:.6f}\n")
            f.write(f"Residual Std: {train_residual_std:.6f}\n\n")
            
            f.write("INTERNAL TEST SET PERFORMANCE (Temporal Generalization):\n")
            f.write(f"MSE: {test_mse:.6f}\n")
            f.write(f"RMSE: {test_rmse:.6f}\n")
            f.write(f"MAE: {test_mae:.6f}\n")
            f.write(f"R2: {test_r2:.6f}\n")
            f.write(f"MAPE: {test_mape:.2f}%\n")
            f.write(f"EVS: {test_evs:.6f}\n")
            f.write(f"Mean Bias (MB): {test_mb:.6f}\n")
            f.write(f"Residual Mean: {test_residual_mean:.6f}\n")
            f.write(f"Residual Std: {test_residual_std:.6f}\n\n")
            
            f.write("EVALUATION SET PERFORMANCE (Campaign Generalization):\n")
            f.write(f"MSE: {eval_mse:.6f}\n")
            f.write(f"RMSE: {eval_rmse:.6f}\n")
            f.write(f"MAE: {eval_mae:.6f}\n")
            f.write(f"R2: {eval_r2:.6f}\n")
            f.write(f"MAPE: {eval_mape:.2f}%\n")
            f.write(f"EVS: {eval_evs:.6f}\n")
            f.write(f"Mean Bias (MB): {eval_mb:.6f}\n")
            f.write(f"Residual Mean: {eval_residual_mean:.6f}\n")
            f.write(f"Residual Std: {eval_residual_std:.6f}\n\n")
            
            f.write("INDIVIDUAL CAMPAIGN PERFORMANCE:\n")
            for campaign, results in campaign_results.items():
                f.write(f"{campaign} ({results['samples']} samples):\n")
                f.write(f"  MSE: {results['mse']:.6f}\n")
                f.write(f"  RMSE: {results['rmse']:.6f}\n")
                f.write(f"  MAE: {results['mae']:.6f}\n")
                f.write(f"  R2: {results['r2']:.6f}\n")
                f.write(f"  MAPE: {results['mape']:.2f}%\n")
                f.write(f"  EVS: {results['evs']:.6f}\n")
                f.write(f"  Mean Bias (MB): {results['mb']:.6f}\n\n")
            
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
        print("- final_xgboost_random_model.pkl (trained model)")
        print("- robust_scaler.pkl (scaler for future predictions)")
        print("- feature_importance.csv (feature rankings)")
        print("- training_set_results.csv (training performance)")
        print("- internal_test_set_results.csv (temporal generalization)")
        print("- evaluation_set_results.csv (campaign generalization)")
        print("- cross_validation_results.csv (CV hyperparameter results)")
        print("- individual_campaign_results.csv (per-campaign performance)")
        print("- dataset_info.csv (dataset summary)")
        print("- random_based_complete_report.txt (comprehensive report)")

        print("\n" + "="*70)
        print("RANDOM-BASED COMPLETE PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*70)
        print("\nSUMMARY:")
        print(f"- Preprocessing: RobustScaler applied to all features")
        print(f"- Hyperparameter tuning: {'Bayesian Optimization' if use_bayesian else 'Grid Search'} with 10-fold CV")
        print(f"- Cross-validation RMSE: {best_cv_score:.6f}")
        print(f"- Training performance: RMSE = {train_rmse:.6f}, R² = {train_r2:.6f}")
        print(f"- Internal test performance (temporal): RMSE = {test_rmse:.6f}, R² = {test_r2:.6f}")
        print(f"- Evaluation performance (campaign): RMSE = {eval_rmse:.6f}, R² = {eval_r2:.6f}")
        print(f"- Individual campaign results saved for detailed analysis")
        print("="*70)

        print("\nTo run this code:")
        print("1. Install required packages: pip install optuna xgboost scikit-learn pandas numpy joblib")
        print("2. Ensure your random-based split CSV files are in the correct directory")
        print("3. Run the script and check the outputs/ folder for comprehensive results")
        print("\nAll performance metrics include: MSE, RMSE, MAE, R², MAPE, EVS, Mean Bias (MB)")