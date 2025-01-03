from aws_s3.connect_s3 import S3Manager
import pandas as pd

def save_clustering_results(cluster_results, model_name="kmeans"):
    """
    Save clustering results to S3 in the AI/clustering folder.
    
    Args:
        cluster_results: Dictionary containing clustering results
            Expected format:
            {
                'labels': array of cluster labels,
                'centers': array of cluster centers,
                'metrics': dict of evaluation metrics
            }
        model_name (str): Name of clustering model used
    
    Returns:
        str: S3 key where results were saved
    """
    s3_manager = S3Manager()
    
    # Convert results to DataFrame if needed
    if isinstance(cluster_results.get('labels'), (list, pd.Series)):
        results_df = pd.DataFrame({
            'cluster_label': cluster_results['labels']
        })
        if 'data' in cluster_results:
            for col, values in cluster_results['data'].items():
                results_df[col] = values
                
        filename = f"{model_name}_clusters.csv"
        return s3_manager.save_ai_results('clustering', results_df, filename)
    else:
        filename = f"{model_name}_results.json"
        return s3_manager.save_ai_results('clustering', cluster_results, filename)

# Example usage:
if __name__ == "__main__":
    # Example clustering results
    example_results = {
        'labels': [0, 1, 0, 2, 1],
        'centers': [[1.0, 2.0], [2.0, 3.0], [0.0, 1.0]],
        'metrics': {
            'silhouette_score': 0.75,
            'calinski_harabasz_score': 120.5
        },
        'data': {
            'feature1': [1.2, 2.3, 1.1, 0.5, 2.1],
            'feature2': [2.1, 3.2, 2.0, 1.1, 3.0]
        }
    }
    
    # Save results
    s3_key = save_clustering_results(example_results, "kmeans")
    print(f"Results saved to: {s3_key}")
