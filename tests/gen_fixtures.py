import time

from sqlglot.optimizer import optimize

TPCH_SCHEMA = {
    "lineitem": {
        "l_orderkey": "bigint",
        "l_partkey": "bigint",
        "l_suppkey": "bigint",
        "l_linenumber": "bigint",
        "l_quantity": "double",
        "l_extendedprice": "double",
        "l_discount": "double",
        "l_tax": "double",
        "l_returnflag": "string",
        "l_linestatus": "string",
        "l_shipdate": "string",
        "l_commitdate": "string",
        "l_receiptdate": "string",
        "l_shipinstruct": "string",
        "l_shipmode": "string",
        "l_comment": "string",
    },
    "orders": {
        "o_orderkey": "bigint",
        "o_custkey": "bigint",
        "o_orderstatus": "string",
        "o_totalprice": "double",
        "o_orderdate": "string",
        "o_orderpriority": "string",
        "o_clerk": "string",
        "o_shippriority": "int",
        "o_comment": "string",
    },
    "customer": {
        "c_custkey": "bigint",
        "c_name": "string",
        "c_address": "string",
        "c_nationkey": "bigint",
        "c_phone": "string",
        "c_acctbal": "double",
        "c_mktsegment": "string",
        "c_comment": "string",
    },
    "part": {
        "p_partkey": "bigint",
        "p_name": "string",
        "p_mfgr": "string",
        "p_brand": "string",
        "p_type": "string",
        "p_size": "int",
        "p_container": "string",
        "p_retailprice": "double",
        "p_comment": "string",
    },
    "supplier": {
        "s_suppkey": "bigint",
        "s_name": "string",
        "s_address": "string",
        "s_nationkey": "bigint",
        "s_phone": "string",
        "s_acctbal": "double",
        "s_comment": "string",
    },
    "partsupp": {
        "ps_partkey": "bigint",
        "ps_suppkey": "bigint",
        "ps_availqty": "int",
        "ps_supplycost": "double",
        "ps_comment": "string",
    },
    "nation": {
        "n_nationkey": "bigint",
        "n_name": "string",
        "n_regionkey": "bigint",
        "n_comment": "string",
    },
    "region": {
        "r_regionkey": "bigint",
        "r_name": "string",
        "r_comment": "string",
    },
}

TPCDS_SCHEMA = {
    "catalog_sales": {
        "cs_sold_date_sk": "bigint",
        "cs_sold_time_sk": "bigint",
        "cs_ship_date_sk": "bigint",
        "cs_bill_customer_sk": "bigint",
        "cs_bill_cdemo_sk": "bigint",
        "cs_bill_hdemo_sk": "bigint",
        "cs_bill_addr_sk": "bigint",
        "cs_ship_customer_sk": "bigint",
        "cs_ship_cdemo_sk": "bigint",
        "cs_ship_hdemo_sk": "bigint",
        "cs_ship_addr_sk": "bigint",
        "cs_call_center_sk": "bigint",
        "cs_catalog_page_sk": "bigint",
        "cs_ship_mode_sk": "bigint",
        "cs_warehouse_sk": "bigint",
        "cs_item_sk": "bigint",
        "cs_promo_sk": "bigint",
        "cs_order_number": "bigint",
        "cs_quantity": "bigint",
        "cs_wholesale_cost": "double",
        "cs_list_price": "double",
        "cs_sales_price": "double",
        "cs_ext_discount_amt": "double",
        "cs_ext_sales_price": "double",
        "cs_ext_wholesale_cost": "double",
        "cs_ext_list_price": "double",
        "cs_ext_tax": "double",
        "cs_coupon_amt": "double",
        "cs_ext_ship_cost": "double",
        "cs_net_paid": "double",
        "cs_net_paid_inc_tax": "double",
        "cs_net_paid_inc_ship": "double",
        "cs_net_paid_inc_ship_tax": "double",
        "cs_net_profit": "double",
    },
    "catalog_returns": {
        "cr_returned_date_sk": "bigint",
        "cr_returned_time_sk": "bigint",
        "cr_item_sk": "bigint",
        "cr_refunded_customer_sk": "bigint",
        "cr_refunded_cdemo_sk": "bigint",
        "cr_refunded_hdemo_sk": "bigint",
        "cr_refunded_addr_sk": "bigint",
        "cr_returning_customer_sk": "bigint",
        "cr_returning_cdemo_sk": "bigint",
        "cr_returning_hdemo_sk": "bigint",
        "cr_returning_addr_sk": "bigint",
        "cr_call_center_sk": "bigint",
        "cr_catalog_page_sk": "bigint",
        "cr_ship_mode_sk": "bigint",
        "cr_warehouse_sk": "bigint",
        "cr_reason_sk": "bigint",
        "cr_order_number": "bigint",
        "cr_return_quantity": "bigint",
        "cr_return_amount": "double",
        "cr_return_tax": "double",
        "cr_return_amt_inc_tax": "double",
        "cr_fee": "double",
        "cr_return_ship_cost": "double",
        "cr_refunded_cash": "double",
        "cr_reversed_charge": "double",
        "cr_store_credit": "double",
        "cr_net_loss": "double",
    },
    "inventory": {
        "inv_date_sk": "bigint",
        "inv_item_sk": "bigint",
        "inv_warehouse_sk": "bigint",
        "inv_quantity_on_hand": "bigint",
    },
    "store_sales": {
        "ss_sold_date_sk": "bigint",
        "ss_sold_time_sk": "bigint",
        "ss_item_sk": "bigint",
        "ss_customer_sk": "bigint",
        "ss_cdemo_sk": "bigint",
        "ss_hdemo_sk": "bigint",
        "ss_addr_sk": "bigint",
        "ss_store_sk": "bigint",
        "ss_promo_sk": "bigint",
        "ss_ticket_number": "bigint",
        "ss_quantity": "bigint",
        "ss_wholesale_cost": "double",
        "ss_list_price": "double",
        "ss_sales_price": "double",
        "ss_ext_discount_amt": "double",
        "ss_ext_sales_price": "double",
        "ss_ext_wholesale_cost": "double",
        "ss_ext_list_price": "double",
        "ss_ext_tax": "double",
        "ss_coupon_amt": "double",
        "ss_net_paid": "double",
        "ss_net_paid_inc_tax": "double",
        "ss_net_profit": "double",
    },
    "store_returns": {
        "sr_returned_date_sk": "bigint",
        "sr_return_time_sk": "bigint",
        "sr_item_sk": "bigint",
        "sr_customer_sk": "bigint",
        "sr_cdemo_sk": "bigint",
        "sr_hdemo_sk": "bigint",
        "sr_addr_sk": "bigint",
        "sr_store_sk": "bigint",
        "sr_reason_sk": "bigint",
        "sr_ticket_number": "bigint",
        "sr_return_quantity": "bigint",
        "sr_return_amt": "double",
        "sr_return_tax": "double",
        "sr_return_amt_inc_tax": "double",
        "sr_fee": "double",
        "sr_return_ship_cost": "double",
        "sr_refunded_cash": "double",
        "sr_reversed_charge": "double",
        "sr_store_credit": "double",
        "sr_net_loss": "double",
    },
    "web_sales": {
        "ws_sold_date_sk": "bigint",
        "ws_sold_time_sk": "bigint",
        "ws_ship_date_sk": "bigint",
        "ws_item_sk": "bigint",
        "ws_bill_customer_sk": "bigint",
        "ws_bill_cdemo_sk": "bigint",
        "ws_bill_hdemo_sk": "bigint",
        "ws_bill_addr_sk": "bigint",
        "ws_ship_customer_sk": "bigint",
        "ws_ship_cdemo_sk": "bigint",
        "ws_ship_hdemo_sk": "bigint",
        "ws_ship_addr_sk": "bigint",
        "ws_web_page_sk": "bigint",
        "ws_web_site_sk": "bigint",
        "ws_ship_mode_sk": "bigint",
        "ws_warehouse_sk": "bigint",
        "ws_promo_sk": "bigint",
        "ws_order_number": "bigint",
        "ws_quantity": "bigint",
        "ws_wholesale_cost": "double",
        "ws_list_price": "double",
        "ws_sales_price": "double",
        "ws_ext_discount_amt": "double",
        "ws_ext_sales_price": "double",
        "ws_ext_wholesale_cost": "double",
        "ws_ext_list_price": "double",
        "ws_ext_tax": "double",
        "ws_coupon_amt": "double",
        "ws_ext_ship_cost": "double",
        "ws_net_paid": "double",
        "ws_net_paid_inc_tax": "double",
        "ws_net_paid_inc_ship": "double",
        "ws_net_paid_inc_ship_tax": "double",
        "ws_net_profit": "double",
    },
    "web_returns": {
        "wr_returned_date_sk": "bigint",
        "wr_returned_time_sk": "bigint",
        "wr_item_sk": "bigint",
        "wr_refunded_customer_sk": "bigint",
        "wr_refunded_cdemo_sk": "bigint",
        "wr_refunded_hdemo_sk": "bigint",
        "wr_refunded_addr_sk": "bigint",
        "wr_returning_customer_sk": "bigint",
        "wr_returning_cdemo_sk": "bigint",
        "wr_returning_hdemo_sk": "bigint",
        "wr_returning_addr_sk": "bigint",
        "wr_web_page_sk": "bigint",
        "wr_reason_sk": "bigint",
        "wr_order_number": "bigint",
        "wr_return_quantity": "bigint",
        "wr_return_amt": "double",
        "wr_return_tax": "double",
        "wr_return_amt_inc_tax": "double",
        "wr_fee": "double",
        "wr_return_ship_cost": "double",
        "wr_refunded_cash": "double",
        "wr_reversed_charge": "double",
        "wr_account_credit": "double",
        "wr_net_loss": "double",
    },
    "call_center": {
        "cc_call_center_sk": "bigint",
        "cc_call_center_id": "string",
        "cc_rec_start_date": "string",
        "cc_rec_end_date": "string",
        "cc_closed_date_sk": "bigint",
        "cc_open_date_sk": "bigint",
        "cc_name": "string",
        "cc_class": "string",
        "cc_employees": "bigint",
        "cc_sq_ft": "bigint",
        "cc_hours": "string",
        "cc_manager": "string",
        "cc_mkt_id": "bigint",
        "cc_mkt_class": "string",
        "cc_mkt_desc": "string",
        "cc_market_manager": "string",
        "cc_division": "bigint",
        "cc_division_name": "string",
        "cc_company": "bigint",
        "cc_company_name": "string",
        "cc_street_number": "string",
        "cc_street_name": "string",
        "cc_street_type": "string",
        "cc_suite_number": "string",
        "cc_city": "string",
        "cc_county": "string",
        "cc_state": "string",
        "cc_zip": "string",
        "cc_country": "string",
        "cc_gmt_offset": "double",
        "cc_tax_percentage": "double",
    },
    "catalog_page": {
        "cp_catalog_page_sk": "bigint",
        "cp_catalog_page_id": "string",
        "cp_start_date_sk": "bigint",
        "cp_end_date_sk": "bigint",
        "cp_department": "string",
        "cp_catalog_number": "bigint",
        "cp_catalog_page_number": "bigint",
        "cp_description": "string",
        "cp_type": "string",
    },
    "customer": {
        "c_customer_sk": "bigint",
        "c_customer_id": "string",
        "c_current_cdemo_sk": "bigint",
        "c_current_hdemo_sk": "bigint",
        "c_current_addr_sk": "bigint",
        "c_first_shipto_date_sk": "bigint",
        "c_first_sales_date_sk": "bigint",
        "c_salutation": "string",
        "c_first_name": "string",
        "c_last_name": "string",
        "c_preferred_cust_flag": "string",
        "c_birth_day": "bigint",
        "c_birth_month": "bigint",
        "c_birth_year": "bigint",
        "c_birth_country": "string",
        "c_login": "string",
        "c_email_address": "string",
        "c_last_review_date": "string",
    },
    "customer_address": {
        "ca_address_sk": "bigint",
        "ca_address_id": "string",
        "ca_street_number": "string",
        "ca_street_name": "string",
        "ca_street_type": "string",
        "ca_suite_number": "string",
        "ca_city": "string",
        "ca_county": "string",
        "ca_state": "string",
        "ca_zip": "string",
        "ca_country": "string",
        "ca_gmt_offset": "double",
        "ca_location_type": "string",
    },
    "customer_demographics": {
        "cd_demo_sk": "bigint",
        "cd_gender": "string",
        "cd_marital_status": "string",
        "cd_education_status": "string",
        "cd_purchase_estimate": "bigint",
        "cd_credit_rating": "string",
        "cd_dep_count": "bigint",
        "cd_dep_employed_count": "bigint",
        "cd_dep_college_count": "bigint",
    },
    "date_dim": {
        "d_date_sk": "bigint",
        "d_date_id": "string",
        "d_date": "string",
        "d_month_seq": "bigint",
        "d_week_seq": "bigint",
        "d_quarter_seq": "bigint",
        "d_year": "bigint",
        "d_dow": "bigint",
        "d_moy": "bigint",
        "d_dom": "bigint",
        "d_qoy": "bigint",
        "d_fy_year": "bigint",
        "d_fy_quarter_seq": "bigint",
        "d_fy_week_seq": "bigint",
        "d_day_name": "string",
        "d_quarter_name": "string",
        "d_holiday": "string",
        "d_weekend": "string",
        "d_following_holiday": "string",
        "d_first_dom": "bigint",
        "d_last_dom": "bigint",
        "d_same_day_ly": "bigint",
        "d_same_day_lq": "bigint",
        "d_current_day": "string",
        "d_current_week": "string",
        "d_current_month": "string",
        "d_current_quarter": "string",
        "d_current_year": "string",
    },
    "household_demographics": {
        "hd_demo_sk": "bigint",
        "hd_income_band_sk": "bigint",
        "hd_buy_potential": "string",
        "hd_dep_count": "bigint",
        "hd_vehicle_count": "bigint",
    },
    "income_band": {
        "ib_income_band_sk": "bigint",
        "ib_lower_bound": "bigint",
        "ib_upper_bound": "bigint",
    },
    "item": {
        "i_item_sk": "bigint",
        "i_item_id": "string",
        "i_rec_start_date": "string",
        "i_rec_end_date": "string",
        "i_item_desc": "string",
        "i_current_price": "double",
        "i_wholesale_cost": "double",
        "i_brand_id": "bigint",
        "i_brand": "string",
        "i_class_id": "bigint",
        "i_class": "string",
        "i_category_id": "bigint",
        "i_category": "string",
        "i_manufact_id": "bigint",
        "i_manufact": "string",
        "i_size": "string",
        "i_formulation": "string",
        "i_color": "string",
        "i_units": "string",
        "i_container": "string",
        "i_manager_id": "bigint",
        "i_product_name": "string",
    },
    "promotion": {
        "p_promo_sk": "bigint",
        "p_promo_id": "string",
        "p_start_date_sk": "bigint",
        "p_end_date_sk": "bigint",
        "p_item_sk": "bigint",
        "p_cost": "double",
        "p_response_target": "bigint",
        "p_promo_name": "string",
        "p_channel_dmail": "string",
        "p_channel_email": "string",
        "p_channel_catalog": "string",
        "p_channel_tv": "string",
        "p_channel_radio": "string",
        "p_channel_press": "string",
        "p_channel_event": "string",
        "p_channel_demo": "string",
        "p_channel_details": "string",
        "p_purpose": "string",
        "p_discount_active": "string",
    },
    "reason": {
        "r_reason_sk": "bigint",
        "r_reason_id": "string",
        "r_reason_desc": "string",
    },
    "ship_mode": {
        "sm_ship_mode_sk": "bigint",
        "sm_ship_mode_id": "string",
        "sm_type": "string",
        "sm_code": "string",
        "sm_carrier": "string",
        "sm_contract": "string",
    },
    "store": {
        "s_store_sk": "bigint",
        "s_store_id": "string",
        "s_rec_start_date": "string",
        "s_rec_end_date": "string",
        "s_closed_date_sk": "bigint",
        "s_store_name": "string",
        "s_number_employees": "bigint",
        "s_floor_space": "bigint",
        "s_hours": "string",
        "s_manager": "string",
        "s_market_id": "bigint",
        "s_geography_class": "string",
        "s_market_desc": "string",
        "s_market_manager": "string",
        "s_division_id": "bigint",
        "s_division_name": "string",
        "s_company_id": "bigint",
        "s_company_name": "string",
        "s_street_number": "string",
        "s_street_name": "string",
        "s_street_type": "string",
        "s_suite_number": "string",
        "s_city": "string",
        "s_county": "string",
        "s_state": "string",
        "s_zip": "string",
        "s_country": "string",
        "s_gmt_offset": "double",
        "s_tax_precentage": "double",
    },
    "time_dim": {
        "t_time_sk": "bigint",
        "t_time_id": "string",
        "t_time": "bigint",
        "t_hour": "bigint",
        "t_minute": "bigint",
        "t_second": "bigint",
        "t_am_pm": "string",
        "t_shift": "string",
        "t_sub_shift": "string",
        "t_meal_time": "string",
    },
    "warehouse": {
        "w_warehouse_sk": "bigint",
        "w_warehouse_id": "string",
        "w_warehouse_name": "string",
        "w_warehouse_sq_ft": "bigint",
        "w_street_number": "string",
        "w_street_name": "string",
        "w_street_type": "string",
        "w_suite_number": "string",
        "w_city": "string",
        "w_county": "string",
        "w_state": "string",
        "w_zip": "string",
        "w_country": "string",
        "w_gmt_offset": "double",
    },
    "web_page": {
        "wp_web_page_sk": "bigint",
        "wp_web_page_id": "string",
        "wp_rec_start_date": "string",
        "wp_rec_end_date": "string",
        "wp_creation_date_sk": "bigint",
        "wp_access_date_sk": "bigint",
        "wp_autogen_flag": "string",
        "wp_customer_sk": "bigint",
        "wp_url": "string",
        "wp_type": "string",
        "wp_char_count": "bigint",
        "wp_link_count": "bigint",
        "wp_image_count": "bigint",
        "wp_max_ad_count": "bigint",
    },
    "web_site": {
        "web_site_sk": "bigint",
        "web_site_id": "string",
        "web_rec_start_date": "string",
        "web_rec_end_date": "string",
        "web_name": "string",
        "web_open_date_sk": "bigint",
        "web_close_date_sk": "bigint",
        "web_class": "string",
        "web_manager": "string",
        "web_mkt_id": "bigint",
        "web_mkt_class": "string",
        "web_mkt_desc": "string",
        "web_market_manager": "string",
        "web_company_id": "bigint",
        "web_company_name": "string",
        "web_street_number": "string",
        "web_street_name": "string",
        "web_street_type": "string",
        "web_suite_number": "string",
        "web_city": "string",
        "web_county": "string",
        "web_state": "string",
        "web_zip": "string",
        "web_country": "string",
        "web_gmt_offset": "string",
        "web_tax_percentage": "double",
    },
}


def rewrite_fixtures(in_path, out_path, schema, num, kind):
    with open(out_path, "w", encoding="utf-8") as fixture:
        for i in range(num):
            i = i + 1
            with open(in_path.format(i=i), encoding="utf-8") as file:
                original = "\n".join(
                    line.rstrip()
                    for line in file.read().split(";")[0].split("\n")
                    if not line.startswith("--")
                )
                original = original.replace("`", '"').strip()
                now = time.time()
                try:
                    optimized = optimize(original, schema=schema)
                except Exception as e:
                    print("****", i, e, "****")
                    continue

                fixture.write(
                    f"""--------------------------------------
-- TPC-{kind} {i}
--------------------------------------
{original};
{optimized.sql(pretty=True)};

"""
                )
                print(i, time.time() - now)


rewrite_fixtures(
    "/home/toby/dev/tpch/{i}.sql",
    "/home/toby/dev/sqlglot/tests/fixtures/optimizer/tpc-h/tpc-h.sql",
    TPCH_SCHEMA,
    22,
    "H",
)

rewrite_fixtures(
    "/home/toby/dev/tpcds/query{i}.sql",
    "/home/toby/dev/sqlglot/tests/fixtures/optimizer/tpc-ds/tpc-ds.sql",
    TPCDS_SCHEMA,
    99,
    "DS",
)
