# MovieETL_Project

This project demonstrates how to clean and transform raw movie ratings and review data using AWS Glue to prepare it for a recommendation system.

## 📌 Objective
To use AWS Glue for extracting, cleaning, transforming, and storing movie data (ratings, metadata, and reviews) in Parquet format.

## 📂 Input Data
- `ratings.csv`: userId, movieId, rating
- `movies.csv`: movieId, title, genres
- `reviews.csv`: userId, movieId, review_text

## 🔄 ETL Process
1. Load CSVs from Amazon S3.
2. Remove missing values.
3. Normalize review text (lowercase, remove special characters).
4. Join datasets on `movieId` and `userId`.
5. Save cleaned data in Parquet format back to S3.

## 🛠️ Tools & Technologies
- AWS Glue
- Amazon S3
- Glue Data Catalog

## 📁 Output
- Cleaned dataset in Parquet format, ready for ML modeling.

## 📸 Screenshots
See the `screenshots/` folder for visuals from the AWS console.

## 📑 Report
Full project documentation is available in the `report/` folder.
