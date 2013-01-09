<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:data="http://java.sun.com/xml/ns/jdbc">
<xsl:param name="sort_column" />
<xsl:param name="sort_order" />
<xsl:template match="data:webRowSet">
	<table class="listtable" cellspacing="0" cellpadding="0">
	<tr>
	<xsl:for-each select="data:metadata/data:column-definition">
		<th class="listhead" align="left">
		<xsl:attribute name="id">column<xsl:value-of select="position()" /></xsl:attribute>
		<pre>
		<xsl:value-of select="data:column-label" />
		</pre>
		</th>
	</xsl:for-each>
	</tr>
	<xsl:for-each select="data:data/data:currentRow">
		<tr>
		<xsl:for-each select="data:columnValue">
			<td class="listbody">
			<xsl:choose>
				<xsl:when test="string-length()=0">
					<pre>-</pre>
				</xsl:when>
				<xsl:otherwise>
					<pre><xsl:value-of select="." /></pre>
				</xsl:otherwise>
			</xsl:choose>					
			</td>
		</xsl:for-each>
		</tr>
	</xsl:for-each>
	</table>
</xsl:template>
</xsl:stylesheet>