# apps/signer — NIE ZAIMPLEMENTOWANY (Etap F)

W tym buildzie nie ma signera, kluczy ani kodu podpisującego. Test
`packages/config/test/no-signer.test.ts` pilnuje, żeby żaden plik produkcyjny nie
importował bibliotek kluczy ani nie wołał metod wysyłających.

Projekt Etapu F (po osobnej zgodzie właściciela):
* osobny proces i osobny sekret; web/API/strategia nie mają dostępu do klucza,
* dedykowany hot wallet z zatwierdzonym małym kapitałem (nigdy główny portfel),
* brak ogólnego endpointu „podpisz base64”; żądania z nonce/TTL, allowlista programów
  z kodu, dekodowanie całej transakcji i ALT, kontrola mintów, kwot, `min_out`,
  odbiorców, tipów, delegacji i authority przed podpisem,
* HALT_SIGNING blokuje wszystko, także sprzedaż,
* przejęcie signera nadal zagraża całemu saldu jego walleta — małe saldo to jedyna twarda granica.
