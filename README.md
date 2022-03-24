SignShop allows you to set up physical shops by punching a chest with your items you want to sell, then punching a sign (while holding redstone dust). It's easy to set up, and even easier to customize!


# Coding Notes for H2 Implementation
~~In H2 mode, some queries (specifically INSERT INTO MetaProperty and INSERT INTO PlayerMeta, which are called from SignShopItemMeta.storeMeta and
PlayerMetadata.setMetavalue respectively) will throw an SQLException as the runSqliteStatement ResultSet genKeys does not contain data [2000-210].
This might be due to the way we call on getInt with RowID in the return for that function, or simply due to the ResultSet not necessarily having to be
filled.~~

Above issue solved as of commit dc6b05b, the issue was not incrementing the ResultSet cursor and thus getting an empty response
