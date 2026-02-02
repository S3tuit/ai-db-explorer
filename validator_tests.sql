Sensitive columns: users.fiscal_code, users.card_code, private.card.balance, expense.receiver.
Safe users-defined functions: users.calc_balance, transfer_amount.

1. ACCEPT – simple select
SELECT u.name
FROM users u
WHERE u.id = 1;

2. ACCEPT – join + group by on non-sensitive
SELECT u.country, COUNT(*)
FROM users u
INNER JOIN expense e ON e.users_id = u.id
WHERE u.status = true
GROUP BY u.country;

3. ACCEPT – order by non-sensitive
SELECT c.id, c.type
FROM card c
WHERE c.type = (SELECT ct.code FROM card_types ct WHERE ct.id = 1)
ORDER BY c.id
LIMIT 50;

4. ACCEPT – safe function on non-sensitive column
SELECT LOWER(u.email) AS email_lc
FROM users u
WHERE u.id = 1;

5. ACCEPT – sensitive appears in SELECT as plain colref + alias tables
SELECT u.fiscal_code, u.name
FROM users u
WHERE u.id = 1
LIMIT 10;

6. ACCEPT – sensitive equality with parameter
SELECT u.id
FROM users u
WHERE u.fiscal_code = $1
LIMIT 200;

7. ACCEPT – sensitive IN with parameters
SELECT u.id
FROM users u
WHERE u.fiscal_code IN ($1, $2, $3)
LIMIT 200;

8. ACCEPT – conjunction of non-sensitive + sensitive predicates
SELECT u.id, u.name
FROM users u
WHERE u.status = $1 AND u.fiscal_code = $2
LIMIT 200;

9. ACCEPT – sensitive predicate + join ON non-sensitive
SELECT u.id, e.amount
FROM users u
INNER JOIN expense e ON e.users_id = u.id
WHERE u.fiscal_code = $1
LIMIT 200;

10. ACCEPT – group by doesn’t touch sensitive columns (sensitive only in WHERE)
SELECT e.category, COUNT(*)
FROM users u
INNER JOIN expense e ON e.users_id = u.id
WHERE u.fiscal_code = $1
GROUP BY e.category
LIMIT 200;

11. REJECT – star
SELECT *
FROM users u
WHERE u.id = 1;

12. REJECT – alias star
SELECT u.*
FROM users u
WHERE u.id = 101;

13. REJECT – star in a join query
SELECT u.name, e.*
FROM users u
INNER JOIN expense e ON e.users_id = u.id
WHERE u.id = $1;

14. REJECT – FROM without alias
SELECT u.name
FROM users
WHERE users.id = 1;

15. REJECT – JOIN without alias
SELECT u.name, e.amount
FROM users u
INNER JOIN expense ON expense.users_id = u.id
WHERE u.id = 1;

16. REJECT – function on sensitive column in SELECT
SELECT LOWER(u.fiscal_code)
FROM users u;

17. REJECT – cast on sensitive column in SELECT
SELECT CAST(u.fiscal_code AS TEXT)
FROM users u
WHERE u.id = 1;

18. REJECT – sensitive compared to literal
SELECT u.id
FROM users u
WHERE u.fiscal_code = 'ABCDEF12G34H567I'
LIMIT 200;

19. REJECT – IN with literal
SELECT u.id
FROM users u
WHERE u.fiscal_code IN ('A', 'B')
LIMIT 200;

20. REJECT – OR
SELECT u.id
FROM users u
WHERE u.fiscal_code = $1 OR u.status = $2
LIMIT 200;

21. REJECT – NOT
SELECT u.id
FROM users u
WHERE NOT (u.fiscal_code = $1)
LIMIT 200;

22. REJECT – inequality
SELECT u.id
FROM users u
WHERE u.fiscal_code <> $1
LIMIT 200;

23. REJECT – LIKE (even with param)
SELECT u.id
FROM users u
WHERE u.fiscal_code LIKE $1
LIMIT 200;

24. REJECT – BETWEEN
SELECT u.id
FROM users u
WHERE u.fiscal_code BETWEEN $1 AND $2
LIMIT 200;

25. REJECT – sensitive in a subquery predicate
SELECT u.id
FROM users u
WHERE u.fiscal_code = (SELECT x.fiscal_code FROM users x WHERE x.id = $1)
LIMIT 200;

26. REJECT – sensitive used in JOIN ON
SELECT u.id, e.amount
FROM users u
INNER JOIN expense e ON e.receiver = u.fiscal_code
WHERE u.id = 1;

27. REJECT – LEFT JOIN
SELECT u.id, e.amount
FROM users u
LEFT JOIN expense e ON e.users_id = u.id
WHERE u.fiscal_code = $1
LIMIT 200;

28. REJECT – CROSS JOIN
SELECT u.id, e.amount
FROM users u
CROSS JOIN expense e
WHERE u.fiscal_code = $1
LIMIT 200;

29. REJECT – ON using OR
SELECT u.id, e.amount
FROM users u
INNER JOIN expense e ON e.users_id = u.id OR e.users_id = $1
WHERE u.fiscal_code = $2
LIMIT 200;

30. REJECT – ON using non-equality
SELECT u.id, e.amount
FROM users u
INNER JOIN expense e ON e.users_id > u.id
WHERE u.fiscal_code = $1
LIMIT 200;

31. REJECT – ON uses function
SELECT u.id, e.amount
FROM users u
INNER JOIN expense e ON LOWER(e.category) = u.status
WHERE u.fiscal_code = $1
LIMIT 200;

32. REJECT – group by sensitive
SELECT u.fiscal_code, COUNT(*)
FROM users u
WHERE u.status = true
GROUP BY u.fiscal_code
LIMIT 200;

33. REJECT – order by sensitive (even if not selected)
SELECT u.id
FROM users u
WHERE u.fiscal_code = $1
ORDER BY u.fiscal_code
LIMIT 200;

34. REJECT – having references sensitive
SELECT u.status, COUNT(*)
FROM users u
WHERE u.status = false
GROUP BY u.status
HAVING MAX(u.fiscal_code) IS NOT NULL
LIMIT 200;

35. ACCEPT – group/having only on non-sensitive
SELECT u.status, COUNT(*)
FROM users u
WHERE u.fiscal_code = $1
GROUP BY u.status
HAVING COUNT(*) > $2
LIMIT 200;

36. REJECT – DISTINCT with sensitive mode trigger
SELECT DISTINCT u.status
FROM users u
WHERE u.fiscal_code = $1;

37. REJECT – OFFSET with sensitive mode trigger
SELECT u.id
FROM users u
WHERE u.fiscal_code = $1
LIMIT 10 OFFSET 10;

38. ACCEPT – DISTINCT when NOT in sensitive mode
SELECT DISTINCT u.status
FROM users u
WHERE u.status = true;

39. ACCEPT – OFFSET when NOT in sensitive mode (if you allow it generally)
SELECT u.id
FROM users u
WHERE u.status = false
LIMIT 10 OFFSET 10;

40. REJECT – sensitive referenced in subquery (CTE)
WITH t AS (
  SELECT u.fiscal_code
  FROM users u
  WHERE u.id = 2
)
SELECT t.fiscal_code
FROM t
LIMIT 10;

41. REJECT – sensitive referenced in scalar subquery
SELECT u.id
FROM users u
WHERE u.id = (SELECT x.id FROM users x WHERE x.fiscal_code = $1)
LIMIT 10;

42. REJECT – sensitive in derived table
SELECT t.id
FROM (SELECT u.id FROM users u WHERE u.fiscal_code = $1) t
LIMIT 10;

43. REJECT – select table value (composite)
SELECT u
FROM users u
WHERE u.id = 1;

44. REJECT – quoted identifier case tricks
SELECT u."fiscal_code"
FROM "users" u
WHERE u."fiscal_code" = $1
LIMIT 10;

45. REJECT – comment injection to fool naive scanners (should still be detected)
SELECT u.fis/*x*/cal_code
FROM users u
WHERE u.fiscal_code = $1
LIMIT 10;

46. REJECT – mixing param and literal in IN
SELECT u.id
FROM users u
WHERE u.fiscal_code IN ($1, 'X')
LIMIT 10;

47. ACCEPT - schema-qualified non sensitive column
SELECT LOWER(c.balance) AS lb
FROM private.card c
LIMIT 10;

48. REJECT - params cannot be selected
SELECT $1 AS trick
FROM users u
LIMIT 1;

49. REJECT - params cannot appear inside FROM VALUES
50. REJECT - params cannot appear inside GROUP BY
51. REJECT - params cannot appear inside HAVING
52. REJECT - params cannot appear inside ORDER BY
53. REJECT - params can only be conpared with sensitive columns
54. REJECT - params cannot appear inside subquery
55. REJECT - param compared to a param
56. ACCEPT - param = sensitive column

57. REJECT – LIMIT must be set in sensitive mode
SELECT u.fiscal_code
FROM users u;

58. REJECT – LIMIT should be lower than 200 in sensitive mode
SELECT u.fiscal_code
FROM users u
LIMIT 201;

59. ACCEPT - Safe users-defined and scope-qualified function
SELECT users.calc_balance(u.id) AS bal
FROM users u
WHERE u.id = 1;

60. REJECT - Unsafe users-defined, not scope-qualified function
SELECT calc_balance(u.id) AS bal
FROM users u
WHERE u.id = 1;

61. ACCEPT - Safe users-defined, global function
SELECT transfer_amount(u.id, (SELECT u2.id FROM users u2 WHERE amount = 123), 50) AS bal
FROM users u
WHERE u.id = 1;

62. REJECT – sensitive in ORDER BY via alias
SELECT u.fiscal_code AS fc
FROM users u
WHERE u.fiscal_code = $1
ORDER BY fc
LIMIT 10;

63. REJECT – sensitive in GROUP BY via alias
SELECT u.fiscal_code AS fc, COUNT(*)
FROM users u
WHERE u.status = true
GROUP BY fc
LIMIT 10;

64. REJECT – sensitive used in SELECT expression
SELECT u.fiscal_code || 'x' AS fc
FROM users u
WHERE u.fiscal_code = $1
LIMIT 10;

65. REJECT – param compared to non-sensitive column
SELECT u.id
FROM users u
WHERE u.status = $1
LIMIT 10;

66. REJECT – param in subquery
SELECT u.id
FROM users u
WHERE u.id = (SELECT x.id FROM users x WHERE x.id = $1)
LIMIT 10;

67. REJECT – sensitive referenced via aggregate alias in HAVING
SELECT u.status, MAX(u.fiscal_code) AS m
FROM users u
GROUP BY u.status
HAVING m IS NOT NULL
LIMIT 10;

68. REJECT – window function orders by sensitive
SELECT row_number() OVER (ORDER BY u.fiscal_code) AS rn
FROM users u
WHERE u.fiscal_code = $1
LIMIT 10;

69. REJECT – sensitive referenced in JOIN ON even with parameter
SELECT u.id, e.amount
FROM users u
INNER JOIN expense e ON e.receiver = $1
WHERE u.fiscal_code = $2
LIMIT 10;

70. REJECT - param inside SELECT
SELECT $1;
