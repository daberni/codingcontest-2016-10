import java.io.File
import java.util.concurrent.LinkedBlockingQueue

const val ORIGIN = "origin"


data class Transaction(val id: String, val inputs: List<TransactionInput>, val outputs: List<TransactionOutput>, val timestamp: Long, var valid: Boolean = false)

data class TransactionInput(val transactionId: String, val owner: String, val amount: Int)

data class TransactionOutput(val owner: String, val amount: Int, var consumed: Boolean = false)

data class TransactionRequest(val transactionId: String, val from: String, val to: String, val amount: Int, val timestamp: Long)


fun main(args: Array<String>) {

    val level = "level4"

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

    val numberOfTransactionRequests = lines.poll().toInt()
    val transactionRequests = (1..numberOfTransactionRequests).map {
        val parts = lines.poll().split(" ")
        TransactionRequest(parts[0], parts[1], parts[2], parts[3].toInt(), parts[4].toLong())
    }


    val validTransactions = transactions.sortedBy { it.timestamp }.filter {
        validate(it, transactions.filter { it.valid })
    }.toMutableList()

    transactionRequests.sortedBy { it.timestamp }.forEach {
        tryProcessTransactionRequest(it, validTransactions)
    }


    val amountNotConsumed = validTransactions.flatMap { it.outputs }.filter { !it.consumed }.sumBy { it.amount }
    val amoutSeeded = validTransactions.flatMap { it.inputs }.filter { it.owner == ORIGIN }.sumBy { it.amount }
    if (amountNotConsumed != amoutSeeded) {
        throw IllegalStateException("amountNotConsumed: $amountNotConsumed != amoutSeeded $amoutSeeded")
    }

    val result = arrayOf(validTransactions.count().toString()) + validTransactions.sortedBy { it.timestamp }.map {
        val inputs = it.inputs.joinToString(" ") { "${it.transactionId} ${it.owner} ${it.amount}" }
        val outputs = it.outputs.joinToString(" ") { "${it.owner} ${it.amount}" }
        "${it.id} ${it.inputs.count()} $inputs ${it.outputs.count()} $outputs ${it.timestamp}"
    }

    return result
}


fun validate(transaction: Transaction, previousTransactions: List<Transaction>): Boolean {
    // sum of inputs = sum of outputs
    if (transaction.inputs.sumBy { it.amount } != transaction.outputs.sumBy { it.amount }) return false

    // owner listed more than once in outputs
    if (transaction.outputs.groupBy { it.owner }.any { it.value.count() > 1 }) return false

    // each amount > 0
    if (transaction.inputs.any { it.amount <= 0 } || transaction.outputs.any { it.amount <= 0 }) return false

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
    if (!allInputsValid) return false

    consumedOutputs.forEach {
        it.consumed = true
    }
    transaction.valid = true
    return true
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

        transactions.add(Transaction(transactionRequest.transactionId, matchingOutputs.map { TransactionInput(it.first, it.second.owner, it.second.amount) }, outputs, transactionRequest.timestamp, true))
        matchingOutputs.forEach {
            it.second.consumed = true
        }
    }
}
