"""Chạy file này trước để tạo sample.xlsx"""
import openpyxl

wb = openpyxl.Workbook()
ws = wb.active
ws.title = "Doanh_thu"

# Header
ws.append(["Tháng", "Sản phẩm", "Doanh thu (VND)", "Số lượng"])

# Data mẫu
data = [
    ["Tháng 1", "Áo", 15_000_000, 100],
    ["Tháng 1", "Quần", 22_000_000, 80],
    ["Tháng 2", "Áo", 18_000_000, 120],
    ["Tháng 2", "Quần", 25_000_000, 90],
    ["Tháng 3", "Áo", 20_000_000, 130],
    ["Tháng 3", "Quần", 30_000_000, 110],
]

for row in data:
    ws.append(row)

wb.save("sample.xlsx")
print("Đã tạo sample.xlsx thành công!")
