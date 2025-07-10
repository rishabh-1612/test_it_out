import time
from models import project_summary_db
from logger import info_logger, error_logger

class UpdateFeatureHierarchyUtils:
    def __init__(self, task_id, files_changed, feature_hierarchy, file_summary_list):
        self.task_id = task_id
        self.files_changed = files_changed
        self.feature_hierarchy = feature_hierarchy
        self.file_summary_list = file_summary_list

        # self.embedding_model = "text-embedding-004" ## Another Embedding Model

    def fetch_feature_hierarchy(self):
        """
        Fetch the feature hierarchy for the given task ID from the MongoDB collection.

        Returns:
            dict: The feature hierarchy for the specified task ID.
        """
        start_time = time.time()
        feature_hierarchy = project_summary_db.projects.find_one(
            {"task_id": self.task_id},
            {
                "feature_view.feature_hierarchy": 1,  # Include this field
                "_id": 0,  # Exclude `_id`
            },
        )
        end_time = time.time()
        print(f"fetch_feature_hierarchy took {end_time - start_time:.4f} seconds")
        return feature_hierarchy

    def fetch_combined_summary(self, file_paths: list):
        """
        Fetch the combined summary for the specified file paths from the MongoDB collection.

        Args:
            file_paths (list): List of file paths to fetch summaries for.

        Returns:
            list: List of dictionaries containing file path and summary data.
        """
        info_logger.info(f"file_paths: {file_paths}")
        info_logger.info(f"file_summary_list: {self.file_summary_list}")
        
        group_paragraph = [
            {"file_path": data["file_path"], "file_summary": data["file_summary"], "git_url": data["git_url"]}
            for data in self.file_summary_list
            if data["file_path"] in file_paths
        ]
        
        return group_paragraph
    
    def find_unique_associated_features(self, search_paths):
        """
        Optimized: Find and collect unique features containing specified search paths.
        Also provides the occurrences of each path in different subfeatures, feature-wise.
        Additionally, includes all file paths used in the feature.

        Args:
            search_paths (list): List of paths to search for.

        Returns:
            list: List of dictionaries containing unique feature objects, path occurrences, and all file paths.
        """
        # Convert search_paths to a set for O(1) lookups
        search_paths_set = set(search_paths)

        # Dictionary to track unique features
        seen_features = {}
        results = []

        for feature in self.feature_hierarchy.get("features", []):
            feature_id = feature.get("feature_id")
            if feature_id in seen_features:
                continue

            # Track occurrences of paths within the feature
            path_occurrences = {}
            all_file_paths = (
                set()
            )  # Use a set to collect all unique file paths in this feature

            for sub_feature in feature.get("sub_features", []):
                for file in sub_feature.get("file_path", []):
                    file_path = file.get("path")
                    # Add the file path to the all_file_paths set
                    all_file_paths.add(file_path)
                    # Check for path existence in O(1)
                    if file_path in search_paths_set:
                        if file_path not in path_occurrences:
                            path_occurrences[file_path] = []
                        path_occurrences[file_path].append(
                            sub_feature.get("name", "Unnamed Subfeature")
                        )

            # Add the feature only if relevant paths were found
            if path_occurrences:
                seen_features[feature_id] = True  # Mark the feature as seen
                results.append(
                    {
                        "feature": feature,
                        "path_occurrences": path_occurrences,
                        "all_file_paths": list(
                            all_file_paths
                        ),  # Convert to list for easier usage
                    }
                )

        return results

    def update_feature_hierarchy(self):
        """
        Handles 'add', 'update', and 'delete' actions for feature data in MongoDB.

        Returns:
            None
        """

        features_to_update = self.find_unique_associated_features(self.files_changed)

        results = []

        for feature in features_to_update:
            all_file_paths = feature.get("all_file_paths")

            group_paragraph = self.fetch_combined_summary(all_file_paths)

            results.append(
                {
                    "feature_id": feature["feature"]["feature_id"],
                    "group_paragraph": group_paragraph,
                }
            )

        return results
