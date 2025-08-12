#!/usr/bin/env python3

from main import clean_up

def test_clean_up():
    # PASTE YOUR SQL QUERIES HERE
    generated_sql = """
    SELECT `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` FROM frpm WHERE `Educational Option Type` = 'Continuation School' AND `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` IS NOT NULL ORDER BY `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` ASC LIMIT 3
    """
    
    gold_sql = """
    SELECT * FROM `K12` table WHERE `5_17` age = 10
    """

    print(clean_up(generated_sql))
    print(clean_up(gold_sql))


if __name__ == "__main__":
    test_clean_up()