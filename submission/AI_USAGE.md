# Khai báo sử dụng AI

Công cụ: **Claude Code** (Anthropic), dùng trong VS Code.

Phạm vi AI đã hỗ trợ:

- Dựng môi trường (venv Python 3.11, cài `requirements.txt`), chạy smoke test, pytest, `run_all.py`
  và thực thi 8 notebook để lưu output.
- Phát hiện và sửa các lỗi trong notebook: cờ schema enforcement hardcode (NB1), lệch múi giờ khi
  tính `date` (NB4), các lỗi đếm/hiển thị ở NB6 (bytes vacuum, checkpoint bị đếm là orphan,
  tên checkpoint sai). Chi tiết ở [INFO.md](INFO.md).
- Soạn nháp các cell "📊 Phân tích kết quả", `INFO.md`, `REFLECTION.md`, bonus `ARCHITECTURE.md` + PoC và script render ảnh bằng
  chứng từ output notebook.

Tôi đã đọc lại các giải thích, đối chiếu với số liệu thực tế trong notebook và chịu trách nhiệm
về nội dung bài nộp. Không có output nào được nhập tay hoặc chỉnh sửa; mọi số liệu đến từ lần
thực thi notebook trên máy cá nhân.
