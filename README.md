# FabricRetailProject
## Dataset

This project uses a synthetic retail transactions dataset sourced from OpenDataBay:

**OpenDataBay dataset page:**  
https://www.opendatabay.com/data/consumer/327c5b3c-9f40-45bb-a79b-d5e2c9abc68a

According to OpenDataBay, this dataset is a **synthetically extended and enriched version** of an original Kaggle dataset.  
While the Kaggle dataset provides the base customer-shopping structure, the OpenDataBay version includes:

- a significantly larger volume (≈300k rows),
- additional transactional attributes (Shipping_Method, Payment_Method, Feedback, Ratings, products),
- extended customer and product metadata,
- normalized date components (Date, Year, Month, Time),
- and enhanced pricing totals (Amount, Total_Amount).

Because of these additions, the OpenDataBay file differs from the Kaggle original in both **schema** and **size**, and should be considered a distinct, synthetic dataset suitable for analytics and engineering demos.

**Referenced Kaggle source (original base dataset):**  
https://www.kaggle.com/datasets/tcsinghal/retail-customer-shopping-dataset

This project uses **the OpenDataBay enriched version**, archived in this repository under GitHub Releases:

👉 **Download dataset (new_retail_data.zip)**  
https://github.com/MrHammer/FabricRetailProject/releases/download/v1.0.0/new_retail_data.zip
