import java.io.File
import java.util.concurrent.LinkedBlockingQueue

const val ORIGIN = "origin"
const val ROOT_BLOCK = "0b00000000"


data class Transaction(val id: String, val inputs: List<TransactionInput>, val outputs: List<TransactionOutput>, val timestamp: Long)

data class TransactionInput(val transactionId: String, val owner: String, val amount: Int)

data class TransactionOutput(val owner: String, val amount: Int, var consumed: Boolean = false)

data class TransactionRequest(val transactionId: String, val from: String, val to: String, val amount: Int, val timestamp: Long)

data class Block(val blockId: String, val previousBlockId: String, val transactionIds: List<String>, val creationTime: Long)

data class ValidBlock(val blockId: String, val previousBlock: ValidBlock?, val transactions: List<Transaction>, val creationTime: Long) {

    val depth: Int
        get() = (previousBlock?.depth ?: 0) + 1

    val blockChain: List<ValidBlock>
        get() = (previousBlock?.blockChain ?: emptyList()) + this
}


fun main(args: Array<String>) {

    val level = "level5"

    File("input").listFiles({ dir, filename -> filename.startsWith(level) }).forEach {
        val result = processFile(it)
        val joined = result.joinToString("\n")

        println("--- OUTPUT ---")
        println(joined)

        File("output/" + it.name).apply {
            delete()
            createNewFile()
            writeText(joined)
        }

        println()
    }
}


fun processFile(file: File): Array<String> {
    println("processing ${file.name}...")

    val lines = LinkedBlockingQueue(file.readLines())

    val numberOfTransactions = lines.poll().toInt()
    val transactions = (1..numberOfTransactions).map {
        val parts = LinkedBlockingQueue(lines.poll().split(" "))
        val id = parts.poll()
        val numberInputs = parts.poll().toInt()
        val inputs = (1..numberInputs).map {
            TransactionInput(parts.poll(), parts.poll(), parts.poll().toInt())
        }
        val numberOutputs = parts.poll().toInt()
        val outputs = (1..numberOutputs).map {
            TransactionOutput(parts.poll(), parts.poll().toInt())
        }

        Transaction(id, inputs, outputs, parts.poll().toLong())
    }

    val numberOfBlocks = lines.poll().toInt()
    val blocks = (1..numberOfBlocks).map {
        val parts = LinkedBlockingQueue(lines.poll().split(" "))
        val blockId = parts.poll()
        val previousBlockId = parts.poll()
        val numberTransactions = parts.poll().toInt()
        val transactionIds = (1..numberTransactions).map { parts.poll() }
        val creationTime = parts.poll().toLong()

        Block(blockId, previousBlockId, transactionIds, creationTime)
    }

    val validTransactions = transactions
            .sortedBy { it.timestamp }
            .fold(emptyList<Transaction>(), { validTransactions, transaction -> (validTransactions + validate(transaction, validTransactions)).filterNotNull() })

    val amountNotConsumed = validTransactions.flatMap { it.outputs }.filter { !it.consumed }.sumBy { it.amount }
    val amoutSeeded = validTransactions.flatMap { it.inputs }.filter { it.owner == ORIGIN }.sumBy { it.amount }
    if (amountNotConsumed != amoutSeeded) {
        throw IllegalStateException("amountNotConsumed: $amountNotConsumed != amoutSeeded $amoutSeeded")
    }


    val validBlocks = blocks
            .sortedBy { it.creationTime }
            .fold(emptyList<ValidBlock>(), { validBlocks, block -> (validBlocks + validateBlock(block, validBlocks, validTransactions)).filterNotNull() })


    val blockChain = validBlocks.sortedWith(compareBy({ it.depth }, { it.creationTime })).lastOrNull()?.blockChain ?: emptyList()
    val blockChainTransactions = blockChain.flatMap { it.transactions }.sortedBy { it.timestamp }

    return arrayOf<String>() +
            blockChainTransactions.count().toString() +
            blockChainTransactions.map {
                val inputs = it.inputs.joinToString(" ") { "${it.transactionId} ${it.owner} ${it.amount}" }
                val outputs = it.outputs.joinToString(" ") { "${it.owner} ${it.amount}" }
                listOf(
                        it.id,
                        it.inputs.count().toString(),
                        inputs,
                        it.outputs.count().toString(),
                        outputs,
                        it.timestamp.toString()
                ).filter(String::isNotEmpty).joinToString(" ")
            } +
            blockChain.count().toString() +
            blockChain.sortedBy { it.creationTime }.map {
                val transactionIds = it.transactions.joinToString(" ") { it.id }
                listOf(
                        it.blockId,
                        it.previousBlock?.blockId ?: ROOT_BLOCK,
                        it.transactions.count().toString(),
                        transactionIds,
                        it.creationTime.toString()
                ).filter(String::isNotEmpty).joinToString(" ")
            }
}


fun validate(transaction: Transaction, previousTransactions: List<Transaction>): Transaction? {
    // sum of inputs = sum of outputs
    if (transaction.inputs.sumBy { it.amount } != transaction.outputs.sumBy { it.amount }) return null

    // owner listed more than once in outputs
    if (transaction.outputs.groupBy { it.owner }.any { it.value.count() > 1 }) return null

    // each amount > 0
    if (transaction.inputs.any { it.amount <= 0 } || transaction.outputs.any { it.amount <= 0 }) return null

    // Input elements must be output elements of previous transactions
    val consumedOutputs = mutableListOf<TransactionOutput>()
    val allInputsValid = transaction.inputs.all { input ->
        if (input.owner == ORIGIN) return@all true

        return@all previousTransactions.any { previousTransaction ->
            val matchingPreviousOutput = previousTransaction.outputs.firstOrNull { output -> !output.consumed && !consumedOutputs.contains(output) && output.owner == input.owner && output.amount == input.amount }
            if (previousTransaction.id == input.transactionId && matchingPreviousOutput != null) {
                consumedOutputs.add(matchingPreviousOutput)
                true
            } else {
                false
            }
        }
    }
    if (!allInputsValid) return null

    consumedOutputs.forEach {
        it.consumed = true
    }
    return transaction
}


fun validateBlock(block: Block, validBlocks: List<ValidBlock>, validTransactions: List<Transaction>): ValidBlock? {
    val previousBlock = validBlocks.singleOrNull { it.blockId == block.previousBlockId }

    if (block.previousBlockId != ROOT_BLOCK && previousBlock == null) return null

    val transactions = block.transactionIds.mapNotNull { transactionId -> validTransactions.firstOrNull { it.id == transactionId } }

    if (transactions.count() != block.transactionIds.count()) return null

    if (block.transactionIds.count() > 20) return null

    if (transactions.any { it.timestamp > block.creationTime }) return null

    val previousTransactions = previousBlock?.blockChain?.flatMap { it.transactions } ?: emptyList()
    val isInPreviousBlock = transactions.flatMap { it.inputs }.all { input -> input.owner == ORIGIN || previousTransactions.any { it.id == input.transactionId } }

    if (!isInPreviousBlock) return null

    return ValidBlock(block.blockId, previousBlock, transactions, block.creationTime)
}


fun tryProcessTransactionRequest(transactionRequest: TransactionRequest, transactions: MutableList<Transaction>) {
    if (transactionRequest.amount == 0) return

    val availableOutputs = transactions
            .filter { it.timestamp < transactionRequest.timestamp }
            .sortedBy { it.timestamp }
            .flatMap { transaction -> transaction.outputs.map { Pair(transaction.id, it) } }
            .filter { it.second.owner == transactionRequest.from && !it.second.consumed }

    var currentAmount = 0
    val matchingOutputs = availableOutputs.takeWhile {
        val required = currentAmount < transactionRequest.amount
        if (required) {
            currentAmount += it.second.amount
        }
        required
    }

    if (currentAmount >= transactionRequest.amount) {
        val outputs = mutableListOf(TransactionOutput(transactionRequest.to, transactionRequest.amount))
        if (currentAmount > transactionRequest.amount) {
            outputs.add(TransactionOutput(transactionRequest.from, currentAmount - transactionRequest.amount))
        }

        transactions.add(Transaction(transactionRequest.transactionId, matchingOutputs.map { TransactionInput(it.first, it.second.owner, it.second.amount) }, outputs, transactionRequest.timestamp))
        matchingOutputs.forEach {
            it.second.consumed = true
        }
    }
}
