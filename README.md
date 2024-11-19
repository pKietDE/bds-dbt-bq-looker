# bds-dbt-bq-looker


## Mực lục 
- [Tổng quan](#tổng-quan)
- [Yêu cầu](#yêu-cầu)
- [Cài đặt](#cài-đặt)
- [Cấu hình](#cấu-hình)
- [Sử dụng](#sử-dụng)
- [luồng dữ liệu](#luồng-dữ-liệu)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Hình ảnh](#hình-ảnh)

## Tổng quan
***Dự án này mục đích xây dựng pipeline để crawl dữ liệu thô từ trang bất động sản về cùng với đó làm việc với Google Cloud để đẩy vào data lake và sử dụng DBT để transform đồng thời sử dụng apache Airflow để thực hiện việc lặp lịch cho dự án***

## Yêu cầu
1. DBT
   - Cần có account GCP vì sử dụng dbt-bigquery .
2. crawl_data
   - Chỉ cần clone về và thực hiện việc [Cấu hình](#cấu-hình) theo hướng dẫn .
3. airflow
  - Chỉ cần clone về và thực hiện việc [Cấu hình](#cấu-hình) theo hướng dẫn .

## Cài đặt
1. Sau khi clone về máy thành công tại thư mục tạo môi trường ảo để tải về các thư viện cần thiết
> python3 -m venv .venv && source .venv/bin/activate  
> pip install folder/requirements.txt # với mỗi folder sẽ có requirements.txt

## Cấu hình
> nano ~/.bashrc  
> export SET_VENV_PRJ_BDS_ARF="/path/to/set_venv/prj_bds_arf.sh"  
> export EXEC_VENV="source .venv/bin/activate"

> [!NOTE]
> Đối với các file còn lại bạn chỉ cần thực hiện cấu hình đúng đường dẫn , và thông tin cần cấu hình
> > export KEY="VALUE"

## Sử dụng 
1. DBT
   - > _# Di chuyển đến thư mục chứa dự án_  
     > cd /path/to/bds_dbt/  
     > _# Thực hiện lệnh run để thực hiện việc chuyển đổi_  
     > dbt run  
2. crawl_data
   - > _# di chuyển đến thư mục chứa dự án_
     > cd /path/to/crawl_data/  
     > _# Thực hiện việc set biến môi trường để chuẩn bị crawl data_  
     > source /path/to/file.sh  
     > _# Chay chương trình _  
     > python3 main.py  
3. airflow
   - > _# khởi tạo cơ sở dữ liệu cho airflow_  
     > airflow db init  
     > _# Điều chỉnh dags_folder trong [core] của airflow là nơi đang chứa code trong thư mục airflow_bds_  
     > vim ~/airflow/airflow.cfg  
   - Khởi tạo user trước khi sử dụng webserver:**_[link](https://airflow.apache.org/docs/apache-airflow/2.0.2/security/webserver.html)_**
   - > _# Khởi tạo Scheduler_  
     > airflow scheduler
   - > _# Khởi tạo webserver_  
     > airflow webserver -p port

## Luồng dữ liệu  
```mermaid
graph TD
    A[Crawl dữ liệu] --> B[Data Lake (GCP)]
    B --> C[DBT (BigQuery)]
    C --> D[Looker Studio]
    A --> E[Apache Airflow]
    E --> B
    E --> C
```

## Hình ảnh
![image](https://github.com/user-attachments/assets/a7cfafad-cd25-4780-87db-8efe7a41326b)


