use near_lake_framework::near_indexer_primitives::near_primitives;

/// This constant represents the block height range.
/// It is set to 500_000, which means that the data splits avery 500_000 blocks.
pub const BLOCK_HEIGHT_RANGE: u64 = 500_000;

/// # Explanation of the logic:
///
/// 1. `block_height / 500000`
///
///     * This integer division truncates any remainder, effectively grouping numbers into bins of 500,000.
///
///     * Example: 73908345 / 500000 = 147 (as integer division discards the remainder).
///
/// 2. `* 5`
///
///     * Since each bin represents a multiple of 500,000, multiplying by 5 scales the result to match the pattern you provided.
///
/// # Example Calculations:
/// Example 1: 73908345
///
///     1. 73908345 / 500000 = 147
///
///     2. 147 * 5 = 735
///
///     Output: 735
///
/// Example 2: 130501000
///     
///     1. 130501000 / 500000 = 261
///
///     2. 261 * 5 = 1305
///
///     Output: 1305
pub async fn get_data_range_id(
    block_height: &near_primitives::types::BlockHeight,
) -> anyhow::Result<u64> {
    Ok((block_height / BLOCK_HEIGHT_RANGE) * 5)
}

/// This function calculates the earliest available block height based on the final block height
pub async fn get_earliest_available_block_height(
    final_block_height: u64,
    keep_data_ranges_number: u64,
) -> anyhow::Result<u64> {
    let final_range_id = get_data_range_id(&final_block_height).await?;
    Ok(final_range_id * 100_000 + keep_data_ranges_number * BLOCK_HEIGHT_RANGE)
}

/// This function checks if the block height is available in the database
/// It returns an error if the block height is less than the earliest available block
pub async fn check_block_height_availability(
    block_height: &near_primitives::types::BlockHeight,
    final_block_height: &near_primitives::types::BlockHeight,
    keep_data_ranges_number: u64,
    archival_mode: bool,
) -> anyhow::Result<()> {
    if archival_mode {
        return Ok(());
    }
    let earliest_available_block =
        get_earliest_available_block_height(*final_block_height, keep_data_ranges_number).await?;
    if *block_height < earliest_available_block {
        anyhow::bail!(
            "The data for block #{} is garbage collected on this node, use an archival node to fetch historical data",
            block_height
        )
    }
    Ok(())
}
