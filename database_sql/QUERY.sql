-- 1/ Hiển thị danh sách gồm: MaSV, HoTen, MaLop, NgaySinh(dd/mm/yyyy), GioiTinh (Nam, Nữ) , Namsinh 
-- những sinh viên có họ không bắt đầu bằng chữ N,L,T.
SELECT MASV as MÃ_SV, HOTEN as HỌ_TÊN, date_format(NGAYSINH,'%d/%m/%Y') as NGÀY_SINH, 
		CASE  
			WHEN GIOITINH = 'False' THEN 'Nam' 
			ELSE 'Nữ'
        END AS GIỚI_TÍNH,
        YEAR(NGAYSINH) AS NĂM_SINH
FROM SINHVIEN
WHERE HOTEN NOT REGEXP '^[NLT]'

-- 2/ Hiển thị danh sách gồm: MaSV, HoTen, MaLop, NgaySinh
-- (dd/mm/yyyy), GioiTinh (Nam, Nữ), Tuổi của những sinh viên có tuổi từ 26-28.
SELECT MASV AS MÃ_SV, 
	   HOTEN AS HỌ_TÊN, 
       MALOP AS MÃ_LỚP,
       date_format(NGAYSINH,'%d/%m/%Y') AS NGÀY_SINH, 
		CASE  
			WHEN GIOITINH = 'False' THEN 'Nam' 
			ELSE 'Nữ'
        END 	AS GIỚI_TÍNH,
        YEAR(NOW()) - YEAR(NGAYSINH) AS TUỔI
FROM SINHVIEN
WHERE YEAR(NOW()) - YEAR(NGAYSINH) BETWEEN 26 AND 28

-- 3/ Hiển thị danh sách gồm MaSV, HoTên, MaLop, DiemHP, MaHP của
-- những sinh viên có điểm HP >= 5.

SELECT SV.MASV AS MÃ_SV, 
	   SV.HOTEN AS HỌ_TÊN,
       SV.MALOP AS MÃ_LỚP,
       DHP.DIEMHP AS ĐIỂM_HỌC_PHẦN,
       DHP.MAHP AS MÃ_HỌC_PHẦN
FROM SINHVIEN AS SV 
JOIN DIEMHP AS DHP
	ON SV.MASV = DHP.MASV
WHERE DHP.DIEMHP >= 5

--  4/ Hiển thị danh sách MaSV, HoTen , MaLop, MaHP, DiemHP được sắp
-- xếp theo ưu tiên Mã lớp, Họ tên tăng dần.
SELECT SV.MASV AS MÃ_SV, 
	   SV.HOTEN AS HỌ_TÊN,
       SV.MALOP AS MÃ_LỚP,
       DHP.DIEMHP AS ĐIỂM_HỌC_PHẦN,
       DHP.MAHP AS MÃ_HỌC_PHẦN
FROM SINHVIEN AS SV 
JOIN DIEMHP AS DHP
	ON SV.MASV = DHP.MASV
WHERE DHP.DIEMHP >= 5

-- 5/Hiển thị danh sách gồm MaSV, HoTen, MaLop, DiemHP, MaHP của
-- những sinh viên có điểm HP từ 5 đến 7 ở học kỳ I

SELECT DHP.MASV AS MÃ_SV, SV.HOTEN AS HỌ_TÊN, SV.MALOP AS MÃ_LỚP, DHP.DIEMHP AS ĐIỂM_HP, DHP.MAHP AS MÃ_HP
FROM DIEMHP DHP
JOIN SINHVIEN SV ON SV.MASV = DHP.MASV
JOIN DMHOCPHAN HP ON DHP.MAHP = HP.MAHP
WHERE (DHP.DIEMHP BETWEEN 5 AND 7) AND HP.HOCKY = 1

-- 6/ Hiển thị danh sách sinh viên gồm MaSV, HoTen, MaLop, TenLop,
-- MaKhoa của Khoa có mã CNTT.
SELECT SV.MASV AS MÃ_SV, SV.HOTEN AS HỌ_TÊN, L.MALOP AS MÃ_LỚP, N.MAKHOA AS MÃ_KHOA
FROM DMLOP  L
JOIN SINHVIEN SV ON SV.MALOP = L.MALOP
JOIN DMNGANH N ON N.MANGANH = L.MANGANH 
WHERE N.MAKHOA = 'CNTT'
ORDER BY SV.MASV

-- 7. Cho biết điểm trung bình chung của mỗi sinh viên ở học kỳ 1.
-- DiemTBC =  sum(DiemHP * SoDvht) /  sum(SoDvht)

SELECT DHP.MASV AS MÃ_SV,
	SUM(DIEMHP * SODVHT)/SUM(SODVHT) AS ĐIỂM_TBC
FROM DMHOCPHAN HP
JOIN DIEMHP DHP ON HP.MAHP = DHP.MAHP
WHERE HP.HOCKY = 1
GROUP BY DHP.MASV

-- 8/ Cho biết HoTen sinh viên có ít nhất 2 học phần có điểm <5.
 SELECT SV.MASV AS MÃ_SV,SV.HOTEN AS HỌ_TÊN, COUNT(DHP.DIEMHP) AS ĐIỂM_HP
 FROM DIEMHP DHP
 JOIN SINHVIEN SV ON SV.MASV = DHP.MASV
 JOIN DMHOCPHAN HP ON HP.MAHP = DHP.MAHP
 WHERE DHP.DIEMHP < 5 
 GROUP BY SV.MASV,SV.HOTEN
 HAVING COUNT(DHP.DIEMHP) >= 2


-- 9/ Cho biết MaHP, TenHP có số sinh viên điểm HP <5 nhiều nhất.

SELECT HP.MAHP AS MÃ_HỌC_PHẦN, HP.TENHP AS TÊN_HỌC_PHẦN, COUNT(DHP.DIEMHP) AS ĐIỂM_HP
FROM DMHOCPHAN HP
JOIN DIEMHP DHP ON HP.MAHP = DHP.MAHP
WHERE DHP.DIEMHP < 5
GROUP BY HP.MAHP, HP.TENHP
ORDER BY COUNT(DHP.DIEMHP) DESC
LIMIT 1

-- 10/ Cho biết Họ tên sinh viên CHƯA học học phần có mã ‘001’.
SELECT MASV AS MÃ_SV, HOTEN AS HỌ_TÊN
FROM SINHVIEN
WHERE MASV NOT IN 
		(
		SELECT MASV
	 	FROM DIEMHP 
		WHERE MAHP = 001
        )



