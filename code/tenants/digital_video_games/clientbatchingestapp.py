def process_batch(batch_df):
  # Filter non-purchase, non-voted reviews
  processed_df = batch_df[(batch_df["total_votes"] > 0) & (batch_df["verified_purchase"] == 'Y')]
  processed_df.fillna("", inplace=True)

  return processed_df
