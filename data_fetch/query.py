class QueryProvider:
    def __init__(self):
        # Redshift 쿼리: 교사-부모-자녀 매핑 테이블 쿼리
        self.redshift_qry_prnts_sales_history = '''
        SELECT 
            -- prnts_cstmr_id,--부모ID(고객그룹ID)
            cast(prnts_cstmr_id as varchar),--부모ID(고객그룹ID)
            --R(Recency):최근구매일계산(VARCHAR에서날짜로변환후개월단위로계산하고+1)
            FLOOR(months_between(current_date,TO_DATE(MAX(prf_ymd),'YYYYMMDD')))+1 AS recent_purchase_in_months,--최근구매일을개월단위로계산하고소수점제거후+1
            --F(Frequency):구매횟수계산
            COUNT(*) AS total_purchase_count,--총구매횟수(구매빈도)
            --M(Monetary):총구매액계산
            SUM(ntprc_amt) AS total_purchase_amount,--총구매금액(구매액)
            --추가분석:6개월내구매횟수계산
            SUM(CASE WHEN TO_DATE(prf_ymd,'YYYYMMDD')>=DATE_ADD('month',-12,current_date)THEN 1 ELSE 0 END) AS purchase_freque_last_6_months,--최근6개월내구매횟수
            --추가분석:6개월내구매총액계산
            SUM(CASE WHEN TO_DATE(prf_ymd,'YYYYMMDD')>=DATE_ADD('month',-12,current_date)THEN ntprc_amt ELSE 0 END) AS purchase_cost_last_6_months,--최근6개월내구매액
            --최근구매전집구분및일자(전집데이터)
            MAX(count) AS total_series_count--전집보유수량
        FROM 
            crm_cust_series_ord_hst_all
        WHERE
            TO_DATE(prf_ymd, 'YYYYMMDD') >= DATEADD('year', -2, CURRENT_DATE) -- 현재날자로부터 2년치 데이터 가져옴
        GROUP BY 
            prnts_cstmr_id--부모ID(고객그룹ID)
        ;
        '''

        self.redshift_qry_prnts_remain_points_history = '''
        WITH LatestPoints AS (
            SELECT 
                prnts_cstmr_id,
                remain_point,
                stnd_mt,
                ROW_NUMBER() OVER (PARTITION BY prnts_cstmr_id ORDER BY stnd_mt DESC) AS rn
            FROM 
                CRM_REMAIN_POINT
        )
        SELECT 
            prnts_cstmr_id,
            remain_point
        FROM 
            LatestPoints
        WHERE 
            rn = 1
        ;
        '''


        self.athena_qry_prnts_lounge_activity_history  = '''
        WITH filtered AS (
            SELECT *,
                CASE 
                    WHEN menu_params LIKE 'prdMstCd=%' THEN SUBSTRING(REGEXP_EXTRACT(menu_params, 'prdMstCd=([^&]+)'), 10)
                    ELSE NULL 
                END AS v
            FROM lounge2_menu_history_renewal
        ),
        joined_data AS (
            SELECT 
                p.contract_number, -- 계약코드
                p.parent_number  , -- 부모코드
                p.mem_sap_code, -- 멤버십SAP코드
                h.user_seq, -- 유저ID (라운지만 사용) >> 연결필요
                h.menu_code, -- 메뉴코드
                m.menu_name, -- 메뉴명
                h.menu_params, -- 메뉴파라미터
                mno.mtrl_nm, -- 자재명
                m.menu_contents, -- 메뉴상세
                h.reg_date -- 방문일자 (메뉴진입시간)
            FROM filtered AS h -- 메뉴이용이력
            LEFT JOIN com_maria_bookclub20.tb_lounge2_menu_renewal AS m -- 메뉴정보
                ON h.menu_code = m.menu_code
            LEFT JOIN com_maria_wjtblep.mp01001m AS mno
                ON h.v = mno.mno
            LEFT JOIN (
                SELECT c.contract_number, -- 계약코드
                    c.parent_number, -- 부모코드
                    p.user_seq, -- 라운지회원ID
                    c.mem_sap_code, -- 멤버십SAP코드
                    c.subscriber_type, -- 멤버십 조직구분
                    c.mem_status -- 회원상태
                FROM unity_parent_contract AS c -- 통합>계약
                LEFT JOIN com_maria_bookclub20.tb_unity_parent AS p -- 통합>부모
                    ON c.parent_number = p.parent_number
            ) AS p
                ON h.user_seq = p.user_seq 
            WHERE 1 = 1
            AND p.subscriber_type = 'M' -- 미래
            AND p.mem_status IN ('1', '3') -- 패드계약이 존재하는 고객
            AND m.menu_name != 'NULL' -- 메뉴명이 있는
            AND DATE_PARSE(h.reg_date, '%Y-%m-%d %H:%i:%s') BETWEEN DATE_ADD('year', -2, CURRENT_DATE) AND CURRENT_DATE -- 2년치 데이터 가져오기
        ),
        agg AS (
            SELECT 
                parent_number,
                MAX(reg_date) AS last_visit_date,
                -- 2. '라운지 홈' 접속 빈도 Count
                SUM(CASE WHEN menu_name = '라운지 홈' THEN 1 ELSE 0 END) AS lounge_home_visit_count,
                -- 3. '북클럽몰>검색결과 상품 목록' 검색 빈도 Count
                SUM(CASE WHEN menu_name = '북클럽몰>검색결과 상품 목록' THEN 1 ELSE 0 END) AS search_count_total,
                -- 4. 'mtrl_nm' 기준 콘텐츠 클릭 빈도 Count
                COUNT(mtrl_nm) AS mtrl_search_count,
                -- 5. 'AI독서코칭' 접속 이력 Count
                SUM(CASE WHEN menu_name = 'AI독서코칭' THEN 1 ELSE 0 END) AS ai_reading_coaching_total
            FROM joined_data
            GROUP BY parent_number
        ),
        final_agg AS (
            SELECT 
                parent_number as prnts_cstmr_id,
                -- 1. lounge앱 최근 방문일로부터 몇 달(M) 지났는지 계산
                CAST(date_diff('month', date_parse(last_visit_date, '%Y-%m-%d %H:%i:%s'), current_date) AS INTEGER) AS months_since_last_visit,
                lounge_home_visit_count,
                search_count_total,
                mtrl_search_count,
                ai_reading_coaching_total
            FROM agg
        )
        SELECT * FROM final_agg
        ;
        '''

        # Athena 쿼리: 사용자의 히스토리 정보를 가져오는 쿼리
        self.athena_qry_child_history  = '''
         SELECT 
          c.customer_number AS chldn_cstmr_id, 
          MAX(a.age) as age, 
          MAX(a.age_range) as age_range,
          MAX(a.gender) as gender ,
          MAX(privilege_code) as privilege_code ,
          SUM(CASE WHEN a.rms_action IN ('RMS_EB_RA', 'RMS_MO_DVS') THEN 1 ELSE 0 END) / 
          COUNT(DISTINCT a.stnd_ymd) AS ct_click, -- 하루 평균 클릭 수
          SUM(CASE WHEN a.rms_action IN ('RMS_EB_RE', 'RMS_MO_RE') THEN 1 ELSE 0 END) / 
          COUNT(DISTINCT a.stnd_ymd) AS ct_completed, -- 하루 평균 완료 수
          AVG(CAST(b.book_idx AS DOUBLE)) AS avg_book_idx, -- book_idx 평균
          SUM(CASE WHEN b.cnts_ty_cd IN ('VIDEO') THEN 1 ELSE 0 END) / 
          COUNT(DISTINCT a.stnd_ymd) AS video_count, -- 비디오 카운트
          SUM(case when b.cnts_ty_cd in ('EBOOK') then 1 else 0 end) /    --이북 카운트
          COUNT(distinct a.stnd_ymd) as ebook_count,
          SUM(case when b.cnts_ty_cd in ('PBOOK') then 1 else 0 end) /    --페이퍼북 카운트
          COUNT(distinct a.stnd_ymd) as pbook_count,
          SUM(case when b.cnts_rlm_nm in ('언어') then 1 else 0 end)  /   --언어 카운트
          count(distinct a.stnd_ymd) as  lang,
          sum(case when b.cnts_rlm_nm in ('사회역사') then 1 else 0 end) /  --사회역사 카운트
          count(distinct a.stnd_ymd) as social_history,
          sum(case when b.cnts_rlm_nm in ('백과종합') then 1 else 0 end) /  -- 백과종합 카운트
          count(distinct a.stnd_ymd) as encyclopedia,
          sum(case when b.cnts_rlm_nm in ('수리과학') then 1 else 0 end) /   --수리과학 카운트
          count(distinct a.stnd_ymd) as math_science,
          sum(case when b.cnts_rlm_nm in ('문화예술') then 1 else 0 end) /   --문화예술 카운트
          count(distinct a.stnd_ymd) as culture_art
        FROM
          tb_rms_entity AS a
        LEFT JOIN 
          dim_cnts AS b ON a.book_code = b.book_cd
        LEFT JOIN 
          tb_member as c ON a.member_code  = c.member_code 
        WHERE 
           a.stnd_ymd between '2023-02-12' AND '2024-08-12'
           and a.rms_action IN ('RMS_EB_RA', 'RMS_EB_RE', 'RMS_MO_DVS', 'RMS_MO_RE')
           AND a.media_fm  IN ( 'PDFC', 'PDF', 'CT', 'MP3','MP3T','APPB','ZIP', 'MP4A','MP4C','PDFC' )
           and cast(a.stnd_ymd as date) between date_add('month', -5, current_date) and current_date
        GROUP BY 
          c.customer_number
          ;
        '''


        self.athena_qry_prnts_age_info = '''
        WITH LatestCustomerInfo AS (
            SELECT 
                cu01001m.cstmr_id,
                date_diff(
                    'year',
                    CASE
                        WHEN cu01001m.lgal_brthy = '00000000' THEN NULL
                        ELSE parse_datetime(cu01001m.lgal_brthy, 'YYYYMMDD')
                    END,
                    current_date
                ) AS age,
                cu01001m.sxdn_cd, 
                cu01001m.cstmr_sts_cd, 
                cu01001m.last_updt_dt,
                ROW_NUMBER() OVER (PARTITION BY cu01001m.cstmr_id ORDER BY cu01001m.last_updt_dt DESC) AS rn
            FROM 
               cu01001m AS cu01001m  -- 고객 테이블
            LEFT JOIN 
                .co02001m AS cntrt  -- 계약 테이블
            ON 
                cntrt.prnts_cstmr_id = cu01001m.cstmr_id
            WHERE 
                cntrt.prnts_cstmr_id IS NOT NULL
                AND cntrt.chldn_cstmr_id IS NOT NULL  -- 자녀ID가 존재하는 경우
                AND cntrt.cntrt_sts_cd IN ('1', '3', '4', '6')  -- 계약 상태코드
                AND cntrt.bsns_orgn_scn_cd = '2000'  -- 2000: 미래본부
        )
        SELECT 
            cstmr_id as prnts_cstmr_id,
            age
        FROM 
            LatestCustomerInfo
        WHERE 
            rn = 1
        ;
        '''


    # Redshift 쿼리 반환 메서드
    def get_redshift_prnts_sales_history_query(self):
        return self.redshift_qry_prnts_sales_history

    def get_redshift_prnts_remain_points_query(self):
        return self.redshift_qry_prnts_remain_points_history

    # Athena 쿼리 반환 메서드
    def get_athena_prnts_lounge_activity_query(self):
        return self.athena_qry_prnts_lounge_activity_history

    def get_athena_child_history_query(self):
        return self.athena_qry_child_history

    def get_athena_customer_id_info_query(self):
        return self.athena_qry_customer_id_info

    def get_athena_prnts_age_info_query(self):
        return self.athena_qry_prnts_age_info
