<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/">
        <waybills>
            <xsl:for-each select="/waybills/waybill">
                <waybill>
                    <issuedt>
                        <xsl:value-of select="./@issuedt"/>
                    </issuedt>
                    <number>
                        <xsl:value-of select="./@number"/>
                    </number>
                    <model>
                        <xsl:value-of select="./model"/>
                    </model>
                    <car>
                        <xsl:value-of select="./car"/>
                    </car>
                    <name>
                        <xsl:value-of select="./driver/name"/>
                    </name>
                    <license>
                        <xsl:value-of select="./driver/license"/>
                    </license>
                    <validto>
                        <xsl:value-of select="./driver/validto"/>
                    </validto>
                    <start>
                        <xsl:value-of select="./period/start"/>
                    </start>
                    <stop>
                        <xsl:value-of select="./period/stop"/>
                    </stop>
                </waybill>
            </xsl:for-each>
        </waybills>
    </xsl:template>
</xsl:stylesheet>