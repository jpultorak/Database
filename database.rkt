#lang racket

(require rackunit)

(provide (struct-out column-info)
         (struct-out table)
         (struct-out and-f)
         (struct-out or-f)
         (struct-out not-f)
         (struct-out eq-f)
         (struct-out eq2-f)
         (struct-out lt-f)
         table-insert
         table-project
         table-sort
         table-select
         table-rename
         table-cross-join
         table-natural-join)

(define-struct column-info (name type) #:transparent)

(define-struct table (schema rows) #:transparent)

(define cities
  (table
   (list (column-info 'city    'string)
         (column-info 'country 'string)
         (column-info 'area    'number)
         (column-info 'capital 'boolean))
   (list (list "Wrocław" "Poland"  293 #f)
         (list "Warsaw"  "Poland"  517 #t)
         (list "Poznań"  "Poland"  262 #f)
         (list "Berlin"  "Germany" 892 #t)
         (list "Munich"  "Germany" 310 #f)
         (list "Paris"   "France"  105 #t)
         (list "Rennes"  "France"   50 #f))))

(define countries
  (table
   (list (column-info 'country 'string)
         (column-info 'population 'number))
   (list (list "Poland" 38)
         (list "Germany" 83)
         (list "France" 67)
         (list "Spain" 47))))

(define (empty-table columns) (table columns '()))

(define (get-val name row schema)
  (if (equal? name (column-info-name [car schema]))
      (car row)
      (get-val name [cdr row] [cdr schema])))

;--------TABLE-INSERT-------
(define (number-of-cols tab)
  (length (table-schema tab))) 

(define (it-types schema row)
  (if (null? schema)
      #t
      (let ([type (column-info-type (car schema))])
        (cond
          [(and (equal? 'string type) (not(string? (car row)))) #f]
          [(and (equal? 'number type) (not(number? (car row)))) #f]
          [(and (equal? 'boolean type) (not(boolean? (car row)))) #f]
          [(and (equal? 'symbol type) (not(symbol? (car row)))) #f]
          [else (it-types (cdr schema) (cdr row))]))))
            

; procedura wstawiania nowego wiersza do tabeli
(define (table-insert row tab)
  (if (and (= [length row] [number-of-cols tab]) (it-types [table-schema tab] row))
      (table [table-schema tab] [cons row (table-rows tab)])
      (raise [error "Wrong row format, cannot insert"])
      ))
;-------TABLE-PROJECT-------
(define (inside-list? el xs)
  (foldl (lambda (x tail-res) (or [equal? x el] tail-res)) #f xs))

;bierze listę indeksów i listę xs; zwraca elementy z xs na danych indeksach bez zmienionej kolejności
(define (list-by-id ids xs)
  (define (f ids xs cur-id)
    (if (null? ids)
        '()
        (if (= cur-id [car ids])
            (cons (car xs) (f [cdr ids] [cdr xs] [+ cur-id 1]))
            (f ids (cdr xs) (+ cur-id 1) ))))
  (f ids xs 0))

;funkcja zwraca indeksy kolumn cols w schemacie schema
;O(c^2) -> z hash mapą może być O(c)
(define (get-id cols schema)
  (define (f cols schema cur-id)
    (if (null? schema)
        '()
        (if (inside-list? [column-info-name (car schema)] cols)
            (cons cur-id (f cols (cdr schema) (+ cur-id 1)))
            (f cols (cdr schema) (+ cur-id 1) ))))
  (f cols schema 0))

(define (rows-project cols tab)
  (foldr (lambda (li tail-res) (cons (list-by-id [get-id cols (table-schema tab)] li) tail-res))
         (list)
         (table-rows tab)))

(define (table-project cols tab)
  (define schema (table-schema tab))
 (table (list-by-id [get-id cols schema] schema) (rows-project cols tab)))

;-------TABLE-RENAME-------
(define (schema-rename col ncol schema)
  (foldr [lambda (column tail)(if (equal? col (column-info-name column))
                                  (cons [column-info ncol (column-info-type column)] tail)
                                  (cons column tail))]
           null
           schema))         
(define (table-rename col ncol tab)
  (table (schema-rename col ncol [table-schema tab])(table-rows tab)))

;-------TABLE-SORT-------

(define (compare val1 val2)
  (cond [(number? val1) (< val1 val2)]
        [(string? val1) (string<? val1 val2)]
        [(boolean? val1) (and [not val1] val2)]
        [(symbol? val1) (string<? (symbol->string val1) (symbol->string val2))]))

(define (compare-row row1 row2 col-names schema)
  (if (null? col-names)
      #f
      (let ([val1 {get-val (car col-names) row1 schema}]
            [val2 {get-val (car col-names) row2 schema}])
        (if (equal? val1 val2)
           (compare-row row1 row2 (cdr col-names) schema)
           (compare val1 val2)))))
                                 

; Używamy wbudowanej funkcji sort, przyjmuje ona listę do posortowania
; oraz komparator;
(define (table-sort cols tab)
  ; definiujemy komparator, opierający się na ogólniejszym komparatorze zdefiniowanym wyżej
  (define (comp row1 row2)
    (compare-row row1 row2 cols (table-schema tab)))
  (table [table-schema tab](sort (table-rows tab) comp)))

;-------TABLE-SELECT-------
(define-struct and-f (l r)) 
(define-struct or-f (l r)) 
(define-struct not-f (e)) 
(define-struct eq-f (name val))
(define-struct eq2-f (name name2))
(define-struct lt-f (name val))


(define (eval-form formula row schema)
  (cond [(and-f? formula) (and {eval-form (and-f-l formula) row schema} {eval-form (and-f-r formula) row schema})]
        [(or-f? formula) (or {eval-form (or-f-l formula) row schema} {eval-form (or-f-r formula) row schema})]
        [(not-f? formula) (not {eval-form (not-f-e formula) row schema})]
        [(eq-f? formula) (equal? {get-val (eq-f-name formula) row schema} {eq-f-val formula})]
        [(eq2-f? formula) (equal? {get-val (eq2-f-name formula) row schema} {get-val (eq2-f-name2 formula) row schema})]
        [else (compare {get-val (lt-f-name formula) row schema} {lt-f-val formula})]))

(define (table-select form tab)
  (table (table-schema tab)
         (foldl [lambda (row acc) (if (eval-form form row [table-schema tab]) (cons row acc) acc)]
                '()
                [table-rows tab])))



;-------TABLE-CROSS-JOIN-------
(define (add-row row rows)
  (if (null? rows)
      '()
      (cons (append [car rows] row) (add-row row (cdr rows)))))

(define (table-cross-join-rows rows1 rows2 acc)
  (if (null? rows2)
      acc
     (table-cross-join-rows rows1 {cdr rows2} (append acc [add-row {car rows2} rows1]))))

(define (table-cross-join tab1 tab2)
  (table [append {table-schema tab1} {table-schema tab2}]
         [table-cross-join-rows {table-rows tab1} {table-rows tab2} '()]))



; -------NATURAL JOIN-------
; Zmienna pomocnicza -> nie chcemy żeby przy zmianie nazwy doszło do innej kolizji
(define col-name-suffix "_temporary1")

; Do nazwy kolumny dodajemy suffix
(define (temporary-col-name col-name)
  (string->symbol(string-append [symbol->string col-name] col-name-suffix)))




(define (get-col-names tab)
  (map column-info-name (table-schema tab)))

(define (get-duplicate-cols tab1 tab2)
  (define cols1 (get-col-names tab1))
  (define cols2 (get-col-names tab2))
  (define (it cols1 cols2 acc)
    (if (null? cols1)
        acc
        (if (inside-list? [car cols1] cols2)
            (it [cdr cols1] [cdr cols2] [cons {car cols1} acc])
            (it [cdr cols1] [cdr cols2]  acc))))
  
  (if (<= [length cols1] [length cols2])
      (it cols1 cols2 '())
      (it cols2 cols1 '())))


(define (rename-cols tab1 col-names)
  (if (null? col-names)
      tab1
      (rename-cols (table-rename [car col-names] (temporary-col-name [car col-names]) tab1)
                   (cdr col-names))))
  
         
(define (table-natural-join tab1 tab2)
  (define dupped-cols (get-duplicate-cols tab1 tab2))
  (define res (table-cross-join tab1 (rename-cols tab2 dupped-cols)))
  (define (get-matching-rows tab cols)
    (if (null? cols)
        tab
        (get-matching-rows [table-select (eq2-f {car cols} {temporary-col-name(car cols)}) tab]
                           [cdr cols])))
  (define original-cols (append [get-col-names tab1] [get-col-names tab2]))
  (table-project original-cols(get-matching-rows res dupped-cols)))




; ------------TESTY------------
(define people
  (table
   [list
         (column-info 'name 'string )
         (column-info 'age 'number )
         (column-info 'male 'boolean)
         (column-info 'networth 'number)
         (column-info 'single 'boolean)
         (column-info 'mid-name 'string)]
   '()))

(define ppl1 (table-insert [list "Warsaw" 23 #t -5 #f "A_A"] people))
(define ppl2 (table-insert [list "Wrocław" 99 #f 32300 #f "B_B"]  ppl1))
(define ppl3 (table-insert [list "C" 30 #f 20 #t "C_C"] ppl2))
(define ppl4 (table-insert [list "D" 30 #t 20 #t "A_D"] ppl3))
;odkomentowanie spowoduje błąd "13" -> oczekiwana jest liczba
;(table-insert [list "D" "13" #t 0 #t "D_D"] ppl3)
;(table-insert [list "D" 13 #t 0] ppl3)

;(table-project '(age networth single) ppl4)
; pusta projekcja
;(table-project '() ppl3)
;(table-rename 'age 'wiek ppl4)
;(table-rename 'networth 'waga ppl4)

;(table-sort '() ppl4)
;(table-select (lt-f 'single #f) ppl4)

;(check-equal?
; [length {table-rows (table-cross-join cities countries)}]
; [* (length {table-rows cities}) (length {table-rows countries})])
;(table-cross-join ppl4 (table-rename 'city 'name cities))
;(table-natural-join ppl4 (table-rename 'city 'name cities))
;(table-cross-join people (table-rename 'city 'name cities))
