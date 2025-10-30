use namada_core::masp_primitives::transaction::Transaction as NamadaMaspTransaction;
use namada_sdk::masp::extract_masp_tx;
use namada_tx::Tx as NamadaTx;
use namada_tx::event::MaspTxRef;

use crate::id::Id;

#[derive(Debug, Clone)]
pub struct Transaction {
    pub hash: Id,
    pub masp_tx: NamadaMaspTransaction,
}

impl Transaction {
    pub fn from_namada_tx(
        transaction: &NamadaTx,
        valid_masp_tx_ref: &MaspTxRef,
    ) -> Result<Self, String> {
        let transaction_id = transaction.header_hash();

        let masp_tx =
            extract_masp_tx(transaction, valid_masp_tx_ref).map_err(|err| {
                format!("Failed to parse MASP transaction: {err}")
            })?;

        Ok(Transaction {
            masp_tx,
            hash: Id::from(transaction_id),
        })
    }
}
