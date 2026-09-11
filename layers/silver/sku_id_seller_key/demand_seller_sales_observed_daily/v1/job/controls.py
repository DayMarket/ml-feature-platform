"""Сверить контрольные SKU totals через границы Arrow-порций."""

class SkuControls:
    def __init__(self):
        self.key = None
        self.sku = None
        self.totals = None
        self.sums = [0, 0]
        self.maximums = [0, 0]

    def consume(self, batch):
        columns = ("sku_id", "seller_key", "sales_orders", "sales_order_items",
                   "sku_sales_orders", "sku_sales_order_items")
        for sku, seller, orders, items, total_orders, total_items in zip(
                *(batch[name].to_pylist() for name in columns)):
            key = (sku, seller)
            if self.key is not None and key <= self.key:
                raise ValueError("Повтор или неверный порядок SKU/seller")
            if self.sku != sku:
                self.finish()
                self.sku, self.totals = sku, (total_orders, total_items)
                self.sums, self.maximums = [0, 0], [0, 0]
            if self.totals != (total_orders, total_items):
                raise ValueError("Разные контрольные итоги продавцов одного SKU-дня")
            for i, value in enumerate((orders, items)):
                self.sums[i] += value
                self.maximums[i] = max(self.maximums[i], value)
            self.key = key

    def finish(self):
        if self.sku is not None and any(not low <= total <= high
                for low, total, high in zip(self.maximums, self.totals, self.sums)):
            raise ValueError("Контрольный SKU итог вне границ объединения seller-множеств")
