import java.io.File
import java.util.concurrent.LinkedBlockingQueue

//
data class Account(val name: String, val accountNumber: String, var balance: Long, val overdraftLimit: Long)

data class Transaction(val from: String, val to: String, val amount: Long, val submitTime: Long)

fun main(args: Array<String>) {

    val level = "level2"

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

    val numberOfAccounts = lines.poll().toInt()
    val accounts = (1..numberOfAccounts).map {
        val parts = lines.poll().split(" ")
        Account(parts[0], parts[1], parts[2].toLong(), parts[3].toLong())
    }.filter {
        validateAccountNumber(it.accountNumber)
    }

    val numberOfTransactions = lines.poll().toInt()
    val transactions = (1..numberOfTransactions).map {
        val parts = lines.poll().split(" ")
        Transaction(parts[0], parts[1], parts[2].toLong(), parts[3].toLong())
    }

    transactions.sortedBy { it.submitTime }.forEach {
        processTransaction(it, accounts)
    }

    val result = arrayOf(accounts.count().toString()) + accounts.map { "${it.name} ${it.balance}" }

    return result
}

fun validateAccountNumber(accountNumber: String): Boolean {
    if (!accountNumber.startsWith("CAT")) {
        println("invalid accountNumber, not starting with CAT: " + accountNumber)
        return false
    }

    val checksum = accountNumber.substring(3, 5).toInt()
    val accountId = accountNumber.substring(5)

    if (accountId.matches(Regex("[a-zA-Z]{10}")).not()) {
        println("invalid accountNumber, not 10 characters: " + accountNumber)
        return false
    }

    val asciiCodes = accountId.toList().map { it.toInt() }
    val characterCounts = asciiCodes.groupBy { it }.mapValues { it.value.count() }
    val equalLowercaseUppercase = ('A'.toInt()..'Z'.toInt()).all {
        val uppercaseCount = characterCounts.getOrElse(it, { 0 })
        val lowercaseCount = characterCounts.getOrElse(it + 32, { 0 })

        lowercaseCount == uppercaseCount
    }
    if (!equalLowercaseUppercase) {
        println("invalid accountNumber, no equal count: " + accountNumber)
        return false
    }

    val calculatedChecksum = 98 - "${accountId}CAT00".sumBy { it.toInt() }.rem(97)
    if (calculatedChecksum != checksum) {
        println("invalid accountNumber, invalid calculated checksum: $calculatedChecksum, checksum: $checksum")
        return false
    }

    return true
}

fun processTransaction(transaction: Transaction, accounts: List<Account>) {

    val from = accounts.singleOrNull { it.accountNumber == transaction.from }
    val to = accounts.singleOrNull { it.accountNumber == transaction.to }

    if (from != null && to != null) {
        val targetAmout = from.balance - transaction.amount
        if (targetAmout > -from.overdraftLimit) {
            from.balance -= transaction.amount
            to.balance += transaction.amount
        } else {
            println("target amount of $targetAmout over overdraftLimit ${from.overdraftLimit}")
        }
    }
}
