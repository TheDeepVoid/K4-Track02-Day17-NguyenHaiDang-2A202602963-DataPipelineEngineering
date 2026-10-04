# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Hải Đăng / 2A202602963
**Repo:** https://github.com/TheDeepVoid/K4-Track02-Day17-NguyenHaiDang-2A202602963-DataPipelineEngineering.git
**Commit bài nộp:** 07d7b22
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** OpenCode (Big Pickle) hỗ trợ tìm lỗi và sửa code một cách tối thiểu; không thay đổi verify/tests/data/checksum logic.
**Nguồn tham khảo khác (nếu có):** Bài giảng Day 17, tài liệu trong docs/.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets has exactly one row per ticket_id` và `T-91 shows its latest state` fail (24 rows cho 12 tickets; T-91 giữ trạng thái cũ như low/open). | `gold_feature_daily reconciles with a full recompute from Silver` checksum lệch, và `u05's offline events of 08-12 (arrived 08-15)` bị đếm sai (0 thay vì 5), đồng thời `LOOKBACK_DAYS covers P99 lateness` fail với P99=3.00 < LOOKBACK_DAYS=0. | `deleted ticket T-97 is a tombstone` fail — T-97 vẫn là row không bị đánh dấu xóa (is_deleted=False), còn chứa PII (email/phone) sau khi xử lý Silver; dẫn đến `latest training snapshot excludes deleted ticket T-97` fail và `deletes propagate to RAG index` fail. |
| **Nguyên nhân gốc** | Silver dùng `INSERT` thẳng vào `silver_tickets` thay vì upsert theo khóa ticket_id. Khi CDC gửi nhiều thay đổi trong một batch hoặc khi chạy lại batch cũ sau batch mới, các bản ghi được thêm thành nhiều hàng (append-only), không ghi đè state mới nhất. LSN dùng để chọn latest trong batch nhưng không cập nhật hàng đã tồn tại. | `LOOKBACK_DAYS = 0` trong config, nhưng events về muộn: ví dụ u05's events ngày 08-12 đến 08-15. Gold_feature_daily recompute chỉ xoá và insert window `[day - 0, day]` theo event_date — không bao phủ các event late mà bị ingest muộn, và full recompute từ Silver (tất cả event đã land) sẽ khác window partial build khi lookback quá nhỏ. | Staging đọc CDC chỉ từ `after` và bỏ qua `before`/`key` cho delete; record `op='d'` có `after=null` nên `ticket_id`, user_id, text bị NULL. Vì vậy T-97 không được dựng thành change với đủ thông tin, upsert không tạo tombstone (is_deleted true + field NULL), đồng thời các bảng Gold dựa vào Silver tickets live vẫn thấy T-97. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: thay `INSERT INTO silver_tickets ...` bằng `MERGE INTO silver_tickets` trên `ticket_id`, update khi có LSN mới hơn (hoặc batch cao hơn), set các trường về NULL khi `is_deleted=true`. Cải thiện dedup trong batch bằng QUALIFY với `ORDER BY _lsn DESC, _batch_id DESC, _kafka_offset DESC`. | `pipeline/config.py`: đổi `LOOKBACK_DAYS = 0` → `LOOKBACK_DAYS = 3` để phủ P99 lateness (3.00 ngày). Việc này đảm bảo khi recompute partition, window `[event_date - L, event_date]` vẫn chứa event đã về muộn (tính theo event time) và full recompute từ Silver khớp với partitioned build. | `pipeline/staging.py`: `ticket_changes_sql` đọc ticket_id từ COALESCE(after->ticket_id, before->ticket_id, key->ticket_id) cho delete; các trường user/subject/body/metadata đọc từ COALESCE(after, before); xử lý timestamps khi NULL. Loại bỏ tombstone records đúng cách và giữ _op. Với sửa này, delete T-97 tạo change `is_deleted=true` với đủ key, Silver MERGE set tombstone (is_deleted=True, user_id/subject/body=NULL). |
| **Khái niệm trên slide** | "Silver — Có khoá": một thực thể = một hàng (ticket_id làm khoá), phải ghi đè trạng thái mới nhất (idempotent upsert), không append vô điều kiện. CDC state phải thắng theo thời điểm thay đổi (LSN) và chạy lại phải tái tạo state đúng. | "Data về muộn": đo lateness từ Bronze (`_ingested_at − event_time`) để đặt lookback đúng (measure, don't guess). Feature được tính theo event time, recompute full từ Silver phải khớp với partitioned recompute khi window đủ rộng. | "CDC log-based": delete có `after=null`, lấy key từ before/key, chuyển thành tombstone (soft delete) tại Silver; delete phải lan xuống Gold (training/RAG) — không giữ dữ liệu cá nhân sau khi xoá. |
