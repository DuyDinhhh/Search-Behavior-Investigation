# **Objective**

The objective of this project is to analyze user search behavior over two specified time periods, June and July, to derive insights into their preferences regarding entertainment content. By processing raw search data, we aim to understand the popularity of various TV shows and movies, categorize them into specific genres, and identify any changes or trends in user preferences between the two months.

- Tech stacks: Pyspark, Python, MySQL, Google Studio.

## **Raw data**

- Log data from available PARQUET files.
- Log data schema

```sh
root
 |-- eventID: string (nullable = true)
 |-- datetime: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- keyword: string (nullable = true)
 |-- category: string (nullable = true)
 |-- proxy_isp: string (nullable = true)
 |-- platform: string (nullable = true)
 |-- networkType: string (nullable = true)
 |-- action: string (nullable = true)
 |-- userPlansMap: array (nullable = true)
 |    |-- element: string (containsNull = true)
```

![log_data](https://github.com/taoxintuyenbo/travinhese/blob/main/asset2/logdata.png?raw=true)

## **Processing data**

- Create a function that takes start and end dates as input parameters and generates a date range between those dates. Use this date range to read Parquet files for each date within the range. Select only the columns `user_id `and `keyword`, and filter out any rows where `user_id` is null.
- Utilize a ranking window function to rank the searches for each user based on their frequency. Select the `keyword` with a `rank of 1` for each user, indicating their top search.
- Analyze the `top 20` popular searches in both `June` and `July` to identify common patterns or themes. Based on these patterns, define `categories` for the `keywords` and save in a csv file. Then, read csv file and create a function to categorize each keyword into one of these specified categories.
```sh
Top search June
+--------------------------------------------+-----+
|keyword                                     |count|
+--------------------------------------------+-----+
|Liên Minh Công Lý: Phiên bản của Zack Snyder|4301 |
|thiên nga bóng đêm                          |3444 |
|nữ thanh tra tài ba                         |2694 |
|sao băng                                    |2418 |
|mộng hoa lục                                |2365 |
|tôi thấy hoa vàng trên cỏ xanh              |2062 |
|why her?                                    |1907 |
|fairy tail                                  |1852 |
|bắt ma phá án                               |1667 |
|siêu nhân                                   |1493 |
|yêu nhầm chị dâu                            |1459 |
|yêu trong đau thương                        |1387 |
|vô tình nhặt được tổng tài                  |1383 |
|vẻ đẹp đích thực                            |1232 |
|lấy danh nghĩa người nhà                    |1111 |
|running man                                 |1059 |
|cô nàng trong trắng oh woo ri               |1045 |
|giữa thanh xuân                             |1007 |
|cảnh đẹp ngày vui biết bao giờ              |988  |
|yêu tinh                                    |987  |
+--------------------------------------------+-----+
```

```sh
Top search July
+--------------------------------------------+-----+
|keyword                                     |count|
+--------------------------------------------+-----+
|Liên Minh Công Lý: Phiên bản của Zack Snyder|4030 |
|thiên nga bóng đêm                          |2439 |
|thanh thanh tử khâm                         |2260 |
|tôi thấy hoa vàng trên cỏ xanh              |2100 |
|yêu tinh                                    |1823 |
|vẻ đẹp đích thực                            |1459 |
|con tim sắt đá                              |1447 |
|nhất kiến khuynh tâm                        |1414 |
|fairy tail                                  |1397 |
|siêu nhân                                   |1296 |
|kẻ trộm mặt trăng: minions                  |1198 |
|yêu thầm anh xã                             |1141 |
|chàng hậu                                   |1117 |
|tại sao lại là oh soo jae?                  |1115 |
|sao băng                                    |1015 |
|anna                                        |1012 |
|running man                                 |968  |
|u19                                         |936  |
|minh châu rực rỡ                            |927  |
|yêu nhầm chị dâu                            |924  |
+--------------------------------------------+-----+
```
- Merge the data from `June` and `July` to analyze `customer taste` and `trending type`. Compare the most searched keyword for each user in June and July. If there's a difference, identify it and extract the previous keyword as the `previous` column. If there's no change, keep the category `unchanged`. If there's a change, `replace the category with the most searched keyword from June` and then import to data warehouse(mysql).

By following these steps, I effectively processed the data from Parquet files, analyzed search patterns, and identified any changes or trends in customer behavior between June and July.

## **Clean data**

- Clean data schema:

```sh
root
 |-- user_id: string (nullable = true)
 |-- most_search_june: string (nullable = true)
 |-- category_june: string (nullable = true)
 |-- most_search_july: string (nullable = true)
 |-- category_july: string (nullable = true)
 |-- Customer_taste: string (nullable = false)
```

![clean_data](https://github.com/taoxintuyenbo/travinhese/blob/main/asset2/clean_data.png?raw=true)

## **Visualizing Data with Google Studio**

![visualize_googlestudio](https://github.com/taoxintuyenbo/travinhese/blob/main/asset2/cleandata_chart.png?raw=true)
