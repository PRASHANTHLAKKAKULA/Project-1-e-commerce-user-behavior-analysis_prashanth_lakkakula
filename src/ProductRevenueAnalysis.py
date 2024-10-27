from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import os

class ProductRevenueAnalysis(MRJob):
    product_info = {}

    def configure_args(self):
        super(ProductRevenueAnalysis, self).configure_args()
        self.add_file_arg('--products', help="Path to the products.csv file")

    def load_product_info(self):
        """Load product data from products.csv into a dictionary for quick lookup."""
        with open(self.options.products, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                product_id, product_name, category, price = row[0], row[1], row[2], float(row[3])
                self.product_info[product_id] = (product_name, category, price)

    def mapper_init(self):
        """Load product information on each mapper node."""
        self.load_product_info()

    def mapper_get_revenue(self, _, line):
        """Emit (ProductID, RevenueGenerated) for each transaction record."""
        try:
            fields = next(csv.reader([line]))
            if fields[0] != "TransactionID":  # Skip header if present
                product_id = fields[3]
                revenue = float(fields[5])
                yield product_id, revenue
        except Exception as e:
            self.increment_counter('Error', 'Mapper_Parse_Error', 1)

    def reducer_sum_revenue(self, product_id, revenues):
        """Sum revenue for each product and emit (Category, (ProductName, TotalRevenue, Price))."""
        total_revenue = sum(revenues)
        if product_id in self.product_info:
            product_name, category, price = self.product_info[product_id]
            yield category, (product_name, total_revenue, price)

    def reducer_find_top3(self, category, products):
        """Emit the top 3 products by revenue for each category."""
        top_3_products = sorted(products, key=lambda x: x[1], reverse=True)[:3]
        for product in top_3_products:
            yield category, product

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_get_revenue,
                   reducer=self.reducer_sum_revenue),
            MRStep(reducer=self.reducer_find_top3)
        ]

    def merge_output_to_csv(self, output_dir, final_output):
        """Merge output files in the given directory into a single CSV file."""
        with open(final_output, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Category", "ProductName", "TotalRevenue", "Price"])  # CSV header

            for filename in sorted(os.listdir(output_dir)):
                if filename.startswith('part-'):
                    with open(os.path.join(output_dir, filename), 'r') as partfile:
                        for line in partfile:
                            # Parse each line as "Category\t(ProductName, TotalRevenue, Price)"
                            try:
                                fields = line.strip().split('\t')
                                category = fields[0]
                                product_name, total_revenue, price = eval(fields[1])  # Convert string tuple to actual tuple
                                writer.writerow([category, product_name, total_revenue, price])
                            except Exception as e:
                                print(f"Error parsing line in {filename}: {line}\nError: {e}")

        print(f"Combined output written to {final_output}")

if __name__ == '__main__':
    # Run the MRJob
    job = ProductRevenueAnalysis(args=[
        'input/transactions.csv',
        '--products', 'input/products.csv',
        '--output-dir', 'output_dir'
    ])
    with job.make_runner() as runner:
        runner.run()
    
    # After job completion, combine the output into a single CSV file
    job.merge_output_to_csv('output_dir', 'output4.csv')